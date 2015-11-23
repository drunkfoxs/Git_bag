package spark.pcap.processor

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.slf4j.LoggerFactory
import spark.pcap.io.PcapCustomReceiver
import spark.pcap.packet.Flow
import org.pcap4j.packet.Packet
import org.pcap4j.packet.IpV4Packet
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import spark.topology.objects
import spark.topology.objects.FlowObject
import spark.topology.objects.SplitFlow
import spark.math.nesting.rank.NestingRank
import spark.math.nesting.rank.MinimalRank
import scala.collection.mutable.ArrayBuffer
import spark.pcap.io.IOProcessor
import org.apache.spark.storage.StorageLevel
import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.ProducerConfig
import spark.pcap.io.KafkaPacketProducer

class PcapProcessor {

  val master = "local[4]"
  val dataDir = "data/file.pcap"

  val windowLength = new Duration(30000)
  val slidingIterval = new Duration(10000)

  /*
   * (1) enable this for local run
   */
//  val conf = new SparkConf().setMaster(master).setAppName("PacketProcessing")

  /*
   * (3) enable this for submiting the application to remote spark server
   */
  val conf = new SparkConf().setAppName("PacketProcessing")

  /*
   * (2) enable this for fetching data from kafka
   */
//  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//  conf.set("spark.kryo.registrator", "spark.pcap.processor.KafkaSparkRegistrator")

  val CAPTURE_TYPE_KAFKA = 3;

  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(30))
  val logger = LoggerFactory.getLogger(getClass())

  def packetProcessing(capture_type: Int, source: String) {
    logger.info("Start reading the pcap packets ...")

    val lines = {
      if (capture_type == CAPTURE_TYPE_KAFKA) {
        new KafkaPacketConsumer().consume(ssc, source)
      } else {
        ssc.receiverStream(new PcapCustomReceiver(capture_type, source))
      }
    }

    val ipMappingJson = parse(getClass().getResourceAsStream("/ipmapping.json"))

    val ipMappings = ipMappingJson.asInstanceOf[JObject].values

    /*
     * Step 1: Transform the packet json string to FlowObjec()
     */

    val flowList = lines.map(line => {
      val Obj = new FlowObject()
      var newLine = line
      for (ipMapping <- ipMappings)
        newLine = newLine.replaceAll(ipMapping._1, ipMapping._2.toString())

      val flowObj = Obj.buildFlowObject(newLine)
      List(flowObj)
    })

    /*
     * Step 2: Union the Packet Streams under one sliding window. Reduce the duplicate packets
     */

//    val windowFlowList = flowList.reduceByWindow((a, b) => {
//
//      val c = a.union(b)
//
//      c
//    }, windowLength, slidingIterval)

    val flowObjsWithDuplicateKey = flowList.flatMap(f => f.map(splitFlow => {
      val flowObj = new FlowObject()
      val str = flowObj.buildFlowWithReduceDuplicateKey(splitFlow)
      str
    }))

    val flatFlowObjsWithDuplicateKey = flowObjsWithDuplicateKey.flatMap(f => {
      f.map(splitFlow => splitFlow)
    })

    val flowWithoutDuplicate = flatFlowObjsWithDuplicateKey.reduceByKey((a, b) => a)

    /*
     * Step 3: Construct the "inTime" for every Http request / response. This is
     * done by extracting the tsval from the ACK for those packets.
     */

    val flowObjsWithInTimeKey = flowWithoutDuplicate.flatMap(splitFlow => {
//      PcapProcessor.debug("Splited flows without duplicate: " + splitFlow._2.getSplitFlowAttributes())
      val flowObj = new FlowObject()
      val flow = flowObj.buildFlowWithInTimeKey(splitFlow._2)
      flow
    })
    //flowObjsWithInTimeKey.print()

    /*
     * This group contains ONE request (or response) and a set of its ACKs
     * update the "InTime" for each edge
     */

    val flowObjsWithInTimeKeyGroup = flowObjsWithInTimeKey.groupByKey()

    /*
     * (Debug) print out all the packets under the same InTimeKeyGroup
     */
    //        //Start debugging
    //        
    //            val printflowObjsWithInTimeKeyGroup = flowObjsWithInTimeKeyGroup.map(group => {
    //              PcapProcessor.output("GroupKey = " + group._1 + ", GroupSize = " + group._2.size)
    //              
    //               val it = group._2.iterator
    //              while (it.hasNext){
    //                val splitFlow = it.next()
    //                PcapProcessor.output("The flows under this group: " + splitFlow.getSplitFlowAttributes)
    //              }  
    //              group      
    //            })
    //            printflowObjsWithInTimeKeyGroup.print()
    //        
    //        //End of debugging    

    /*
     * Drop the group which contains just ACKs
     */

    val flowObjsWithInTimeKeyGroupFilted = flowObjsWithInTimeKeyGroup.filter(f => {
      var isNotAllAck = false
      f._2.foreach(ff => {
        if (!ff.getSplitFlowAttributes().getIsAck())
          isNotAllAck = true
      })
      isNotAllAck
    })

    /*
     * Step 3.1 Build the data with <request/response, its ACKs>
     */

    val ackSetsWithRequest = flowObjsWithInTimeKeyGroupFilted.map(f => {
      val o = f._2.filter(!_.getSplitFlowAttributes().getIsAck())

      (o.head, f._2.filter(_.getSplitFlowAttributes().getIsAck()))
    })

    /*
     * Step 3.2 Generate the "inTime" for every Http request / response
     */

    val requestWithInTime = ackSetsWithRequest.map(f => {
      var tsval = -1l
      if (!f._2.isEmpty) {
        val ack = f._2.reduce((a, b) => {
          if (b.getSplitFlowAttributes().getTsVal < a.getSplitFlowAttributes().getTsVal) {
            b
          } else {
            a
          }
        })
        tsval = ack.getSplitFlowAttributes().getTsVal()
      }
//      PcapProcessor.debug("Attributes for this flow: " + f._1.getSplitFlowAttributes() + ", inTime = " + tsval)
      f._1.getSplitFlowEdge().foreach(edge => {
        edge._2.setInTime(tsval)
      })
      f._1
    })

    /*
     * Step 4: Reduce the packets (the request and response pair) with the same tuple
     * For Http Request: <src_ip=$src_ip, src_port=$src_port, dst_ip=$dst_ip, dst_port=$dst_port, time=$tsval>
     * For Http Response: <src_ip=$dst_ip, src_port=$dst_port, dst_ip=$src_ip, dst_port=$src_port, time=$tsecr>
     */

    val flowObjsWithGroupKey = requestWithInTime.flatMap(splitFlow => {
      val flowObj = new FlowObject()
      val flow = flowObj.buildFlowWithGroupKey(splitFlow)
      flow
    })

    /*
     * (Debug) Group the packet with the same tuple
     * For Http Request: <src_ip=$src_ip, src_port=$src_port, dst_ip=$dst_ip, dst_port=$dst_port, time=$tsval>
     * For Http Response: <src_ip=$dst_ip, src_port=$dst_port, dst_ip=$src_ip, dst_port=$src_port, time=$tsecr>
     */
    //        //Start debugging
    //    
    //                val flowsGroupedByKey = flowObjsWithGroupKey.groupByKey
    //                  
    //                val groupedSplitFlows = flowsGroupedByKey.map(group => {
    //                    PcapProcessor.output("GroupKey = " + group._1 + ", GroupSize = " + group._2.size)
    //                    if (group._2.size > 0) {
    //                      /*
    //                       * Iterate the packet pairs in this group
    //                       */
    //                      val it = group._2.iterator
    //                      while (it.hasNext){
    //                        val splitFlow = it.next()
    //                        PcapProcessor.output("The flows under this group: " + splitFlow.getSplitFlowAttributes)
    //                        PcapProcessor.output("The number of this flow's nodes: " + splitFlow.getSplitFlowNodes().size)
    //                        PcapProcessor.output("The number of this flow's edges: " + splitFlow.getSplitFlowEdge().size)
    //                      }
    //                    }        
    //                    group
    //                  })     
    //                  
    //                groupedSplitFlows.print
    //                
    //        //End of debugging          

    val flowsReduceByKey = flowObjsWithGroupKey.reduceByKey((a, b) => {

//      b.getSplitFlowNodes.foreach(f => {
//        a.updateSplitFlowNode(f._1, f._2)
//      })
      b.getSplitFlowEdge.foreach(f => {
        a.updateSplitFlowEdge(f._1, f._2)
      })
//      if (b.getSplitFlowAttributes.getRTT > -1) {
//        a.getSplitFlowAttributes().setRTT(b.getSplitFlowAttributes.getRTT)
//      }
      if (b.getSplitFlowAttributes.getStartTime > -1) {
        a.getSplitFlowAttributes.setStartTime(b.getSplitFlowAttributes.getStartTime)

      }
//      if (b.getSplitFlowAttributes.getEndTime > -1) {
//        a.getSplitFlowAttributes.setEndTime(b.getSplitFlowAttributes.getEndTime)
//      }
      a
    })

    /*
     * Step 5: Update RTT and RTTDST for each Request and Response pair
     */

    val updatedRTT = flowsReduceByKey.map(flow => {
      var startTime = -1l
      var endTime = -1l
      var arriveTime = -1l
      var leaveTime = -1l
      var parentName = ""
      var childName = ""
      var requestURI = ""

      val splitFlow = flow._2
      val splitFlowEdges = splitFlow.getSplitFlowEdge()
      val it = splitFlowEdges.iterator

      while (it.hasNext) {
        val splitFlowEdge = it.next()._2

        /*
         * complement the unknown inTime / outTime with the other one which is not "-1"
         * in this case set inTime = outTime
         */
        if (splitFlowEdge.getInTime() == -1) {
          if (splitFlowEdge.getOutTime() != -1) {
            splitFlowEdge.setInTime(splitFlowEdge.getOutTime())
          }
        } else if (splitFlowEdge.getOutTime() == -1) {
          splitFlowEdge.setOutTime(splitFlowEdge.getInTime())
        }

        if (splitFlowEdge.getIsRequest()) {
          requestURI = splitFlowEdge.getHttpUri()
          
          // iteration over two edges, so check if the value need to update on 2nd iteration 
          if (startTime == -1) {
            startTime = splitFlowEdge.getOutTime()
            arriveTime = splitFlowEdge.getInTime()
            parentName = splitFlowEdge.getInV()
            childName = splitFlowEdge.getOutV()
          } else if (startTime > splitFlowEdge.getOutTime) {
            startTime = splitFlowEdge.getOutTime()
            arriveTime = splitFlowEdge.getInTime()
            parentName = splitFlowEdge.getInV()
            childName = splitFlowEdge.getOutV()
          }
        } else if (splitFlowEdge.getIsResponse()) {
          if (endTime == -1) {
            endTime = splitFlowEdge.getInTime()
            leaveTime = splitFlowEdge.getOutTime()
          } else if (endTime < splitFlowEdge.getInTime()) {
            endTime = splitFlowEdge.getInTime()
            leaveTime = splitFlowEdge.getOutTime()
          }
        }
      }

      splitFlow.getSplitFlowAttributes().setStartTime(startTime)
      splitFlow.getSplitFlowAttributes().setEndTime(endTime)
      splitFlow.getSplitFlowAttributes().setArriveTime(arriveTime)
      splitFlow.getSplitFlowAttributes().setLeaveTime(leaveTime)
      splitFlow.getSplitFlowAttributes().setHttpUri(requestURI)
      if (endTime != -1l && startTime != -1l) {
        splitFlow.getSplitFlowAttributes().setRTT(endTime - startTime)
      }
      if (arriveTime != -1l && leaveTime != -1l) {
        splitFlow.getSplitFlowAttributes().setRTTDst(leaveTime - arriveTime)
      }

      splitFlow.getSplitFlowParentID().setName(parentName)
      splitFlow.getSplitFlowParentID().setParentInTime(arriveTime)
      splitFlow.getSplitFlowParentID().setParentOutTime(leaveTime)

      splitFlow.getSplitFlowChildID().setName(childName)
      splitFlow.getSplitFlowChildID().setChildOutTime(startTime)
      splitFlow.getSplitFlowChildID().setChildInTime(endTime)

//      PcapProcessor.debug("GroupKey = " + flow._1 + ", number of nodes = " + splitFlow.getSplitFlowNodes.size + ", number of edges = " + splitFlow.getSplitFlowEdge.size)
//      PcapProcessor.debug("attribute: " + splitFlow.getSplitFlowAttributes)
//      PcapProcessor.debug("parentID: " + splitFlow.getSplitFlowParentID())
//      PcapProcessor.debug("childID: " + splitFlow.getSplitFlowChildID())
//      splitFlow.getSplitFlowNodes.foreach(f => {
//        PcapProcessor.debug("node: " + f._2)
//      })
//      splitFlow.getSplitFlowEdge.foreach(f => {
//        PcapProcessor.debug("edge: " + f._2)
//      })
      splitFlow
    })

    //updatedRTT.print()

    val result = updatedRTT.map(f => List(f)).reduce((a, b) => a.++(b)).map(splitFlowList => {
      PcapProcessor.debug("splitFlowList size " + splitFlowList.size)

      splitFlowList.map(splitFlow => {

        //        /*
        //         * [Nesting ranking 1]: rank by assuming that the request processing time belongs to normal distribution
        //         */
        //        val rank = new NestingRank()
        /*
         * [Nesting ranking 2]: rank by pick up the node with the minimal response DeltaT
         */
        val rank = new MinimalRank()

        val nextSFList = splitFlowList.filter(p => {

          val currentSFChild = splitFlow.getSplitFlowChildID()
          val otherSFParent = p.getSplitFlowParentID()

          (!splitFlow.equals(p) && currentSFChild.getChildOutTime() != -1
            && otherSFParent.getParentInTime() != -1
            && currentSFChild.getChildInTime() != -1
            && otherSFParent.getParentOutTime() != -1
            && currentSFChild.getName().equals(otherSFParent.getName())
            && currentSFChild.getChildInTime() <= otherSFParent.getParentOutTime()
            && currentSFChild.getChildOutTime >= otherSFParent.getParentInTime)
        })

        val nextSFsWithSore = nextSFList.map(p => {
          val scoreRequest = splitFlow.getSplitFlowChildID().getChildOutTime() - p.getSplitFlowParentID().getParentInTime()
          val scoreResponse = p.getSplitFlowParentID().getParentOutTime() - splitFlow.getSplitFlowChildID().getChildInTime()
          //          /*
          //           * [Nesting ranking 1]: rank by assuming that the request processing time belongs to normal distribution
          //           */
          //          rank.set(scoreRequest)

          /*
           * [Nesting ranking 2]: rank by pick up the node with the minimal response DeltaT
           */
          rank.set(scoreResponse)

//          PcapProcessor.debug("Parent node: " + p.getSplitFlowParentID().getName() + ", parent node in time: " + p.getSplitFlowParentID().getParentInTime() + ", parent node out time: " + p.getSplitFlowParentID().getParentOutTime() + ", parent node URI: " + p.getSplitFlowAttributes().getHttpUri())
//          PcapProcessor.debug("Child node: " + splitFlow.getSplitFlowChildID().getName() + ", child node out time: " + splitFlow.getSplitFlowChildID().getChildOutTime() + ", child node in time: " + splitFlow.getSplitFlowChildID().getChildInTime() + ", child node URI: " + splitFlow.getSplitFlowAttributes().getHttpUri())
//          PcapProcessor.debug("DeltaT for Request: " + scoreRequest + ", DeltaT for Response: " + scoreResponse)
          //        /*
          //         * [Nesting ranking 1]: rank by assuming that the request processing time belongs to normal distribution
          //         */          
          //        (p, scoreRequest)
          /*
           * [Nesting ranking 2]: rank by pick up the node with the minimal response DeltaT
           */
          (p, scoreResponse)
        })

//        PcapProcessor.debug("parentWithSore size: " + nextSFsWithSore.size)

        val highestProbabilityScore = rank.highestProbability()
//        PcapProcessor.debug("highestProbabilityDeltaT: " + highestProbabilityScore)

        // select the parent with the highest probability
        if (nextSFsWithSore.size > 1) {
          val a = nextSFsWithSore.filter(_._2 == highestProbabilityScore).reduce((a, b) => { a })
//          PcapProcessor.output("children size:" + nextSFsWithSore.size)
          ArrayBuffer(a._1, splitFlow)
        } else if (nextSFsWithSore.size == 1) {
          val a = nextSFsWithSore.head
          ArrayBuffer(a._1, splitFlow)
        } else {
          ArrayBuffer(splitFlow)
        }

      })
    })

    // transform ((a,b), (a,c), (b,d), (e,f)) => (((a,b), (a,c), (b,d)),((e,f))) 
    //  => ((a, b, a, c, b, d),(e,f)) 
    val result2 = result.map(f => {

      f.foldLeft(ArrayBuffer[ArrayBuffer[ArrayBuffer[SplitFlow]]]())((z, f) => {

        if (z.size == 0)
          z.+=(ArrayBuffer(f))
        else {
          var p1 = ArrayBuffer[ArrayBuffer[SplitFlow]]()
          var p2 = ArrayBuffer[ArrayBuffer[SplitFlow]]()
          z.foreach(pairList => {

            pairList.foreach(pair => {

              pair.foreach(elem => {

                if ((elem == f.head || elem == f.last)) {
                  if (p1.size == 0)
                    p1 = pairList
                  else
                    p2 = pairList
                }
              })
            })

          })
          if (p1.size == 0) {

            z :+ ArrayBuffer(f)

          } else if (p1 != p2) {

            p1.++=(p2)
            p1.+=(f)

            z.-(p2)

          } else {
            p1.+=(f)
            z
          }
        }

      }).map(f => {
        f.flatMap(list => list.map(splitFlow => splitFlow))

      })
    })

    val result3 = result2.map(f => {
      f.map(zList => {

        val transactionData = zList.distinct.foldLeft(List[JArray]())((z, f) => {
          val attrs = f.getSplitFlowAttributes()

          z :+ JArray(List(attrs.getStartTime(),
            attrs.getEndTime(),
            attrs.getRTT(),
            attrs.getHttpUri(),
            attrs.getSrc(), attrs.getDst(), attrs.getSrcPort().toString()))

        })

        val transactionJsonObj = List(
          ("name" -> "transaction") ~
            ("columns" -> List("startTime", "endTime", "responseTime", "uri", "src", "dest", "port")) ~
            ("points" -> JArray(transactionData)))

        val transactionJson = compact(render(transactionJsonObj))

        val vertices = zList.flatMap(f => {

          f.getSplitFlowNodes().map(node => node)
        }).distinct.map(_._2)

        val verticesJson = vertices.map(node => {
          (
            ("_id" -> node._id) ~
            ("_type" -> node._type) ~
            ("name" -> node.ip) ~
            ("ip" -> node.ip))
        })

        val edges = zList.distinct.map(f => {
          val requestEdge = f.getSplitFlowEdge().filter(!_._2.isResponse)
          val responseEdge = f.getSplitFlowEdge().filter(_._2.isResponse)

          if (!requestEdge.isEmpty && !responseEdge.isEmpty) {
            requestEdge.head._2.setResponseTime(
              responseEdge.head._2.getInTime() - requestEdge.head._2.getOutTime())
            requestEdge
          } else {
            f.getSplitFlowEdge().filter(!_._2.isResponse)
          }

        }).flatMap(f => {
          f.map(edge => edge)
        }).distinct.filter(f => {
          (f._2._label != null && f._2._label != "")
        }).map(_._2)

        // a root vertex is a vertex which without in-edge and has an out-edge at least.
        val verticesWithoutInEdge = vertices.filter(f => {
          if (edges.isEmpty) true else
            !edges.map(edge => {
              edge._inV.equals(f._id)
            }).reduce((a, b) => a || b)
        })

        val rootVertices = if (verticesWithoutInEdge.isEmpty)
          ArrayBuffer()
        else verticesWithoutInEdge.filter(f => {
          true
        })

        val rootVerticesOutEdges = if (rootVertices.isEmpty) ArrayBuffer() else edges.filter(edge => {
          rootVertices.map(f => {
            edge._outV.equals(f._id)
          }).reduce((a, b) => a || b)
        })

        val rootUri = if (rootVerticesOutEdges.isEmpty) "" else rootVerticesOutEdges.head.getHttpUri()
        val rootStartTime = if (rootVerticesOutEdges.isEmpty) -1l else rootVerticesOutEdges.head.getOutTime()
        val rootEndTime = if (rootVerticesOutEdges.isEmpty) -1l else rootVerticesOutEdges.head.getInTime()

        val edgesJson = edges.map(edge => {

          (
            ("_id" -> edge._id) ~
            ("_type" -> edge._type) ~
            ("_inV" -> edge._inV) ~
            ("_outV" -> edge._outV) ~
            ("_label" -> edge._label) ~
            ("protocol" -> edge.protocol) ~
            ("responseTime" -> edge.responseTme) ~
            ("httpMethod" -> edge.httpMethod) ~
            ("inPort" -> edge.inPort) ~
            ("outPort" -> edge.outPort) ~
            ("httpUri" -> edge.httpUri))
        })

        val graphObj = ("edges" -> edgesJson) ~ ("vertices" -> verticesJson)

        val jsonObj = List(("name" -> "topology") ~
          ("columns" -> List("startTime", "endTime", "uri", "graph")) ~
          ("points" -> List(JArray(List(rootStartTime, rootEndTime, rootUri, compact(render(graphObj)))))))

        val json = compact(render(jsonObj))

        List(transactionJson, json)
      })
    })

    val producer = if (capture_type == CAPTURE_TYPE_KAFKA) {

      val props = new java.util.HashMap[String, Object]()
      props.put("metadata.broker.list", source + ":9092");
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, source + ":9092")
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
  
      val producerTopic = "transaction" 
      new KafkaPacketProducer(props, producerTopic)

    }

    result3.foreachRDD(rdd => {
         rdd.collect().foreach(json => {
           PcapProcessor.output("Total recovered topologies: "+json.size)})
      })    
      
    result3.foreachRDD(rdd => {
      rdd.collect().foreach { record =>
        record.flatMap(_.map(str => str)).map(str => {
          
//          PcapProcessor.output("Recovered topology: " + str)
          
          producer match {
            case e : KafkaPacketProducer => e.sendMsg(str)
            case _ =>
              }
          })
        }
      })

    sys.ShutdownHookThread {
      ssc.stop(true, true)
      logger.info("Spark streaming service stopped")
    }

    ssc.start();
    ssc.awaitTermination();

  }

}

object PcapProcessor {
  val logger = LoggerFactory.getLogger(getClass())
  var contextFlows = new HashMap[String, Flow]()
  def output(c: String) {
    logger.info("[PcapProcessor] " + c)
  }
  def debug(c: String) {
    logger.debug("[PcapProcessor] " + c)
  }
}
