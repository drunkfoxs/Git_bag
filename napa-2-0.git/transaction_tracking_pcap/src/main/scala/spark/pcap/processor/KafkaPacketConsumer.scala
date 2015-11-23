package spark.pcap.processor

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import akka.actor.Status.Success
import akka.actor.Status.Failure
import kafka.serializer.StringDecoder
import kafka.serializer.DefaultDecoder
import iodine.avro.TapRecord
import spark.pcap.readers.PcapReader
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory
import iodine.avro.TapRecord

class KafkaPacketConsumer {

  val topics = Array("pymicro")
  val numThreads = 1
  val group = "napa-tcpdump"

  val topicpMap = topics.map((_, numThreads.toInt)).toMap

  def consume(ssc: StreamingContext, source: String): DStream[String] = {

    val kafkaStream = {

      val kafkaParams = Map(
        "metadata.broker.list" -> (source + ":9092"),
        "zookeeper.connect" -> (source + ":2181"),
        "group.id" -> group,
        "zookeeper.connection.timeout.ms" -> "1000")
      //val numPartitionsOfInputTopic = 1
      //val streams = (1 to numPartitionsOfInputTopic) map { _ =>
        KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicpMap, StorageLevel.MEMORY_ONLY_SER)
      //}

      //val unifiedStream = ssc.union(streams)
      //val sparkProcessingParallelism = 1 
      //unifiedStream.repartition(sparkProcessingParallelism)
    }

    kafkaStream.flatMap(f => {
      
      val decoder = new AvroDecoder[TapRecord](TapRecord.getClassSchema())
      val p = decoder.fromBytes(f._2)
      val inputStream = new ByteArrayInputStream(p.getTapData().array())  
      
      val pcapReader = new PcapReader(new DataInputStream(inputStream))
      
      val it = pcapReader.iterator()
      val list = ArrayBuffer[String]()
      while(it.hasNext()) {
        val packet = it.next()
        //logger.info("+++++++++++++++++++++")
        //logger.info(packet.getFlow().flowKeyinJson().toString())
				list.+=(packet.getFlow().flowKeyinJson().toString() + "\n")
      }
      
      list
    })
    
  }
  //val logger = LoggerFactory.getLogger(getClass())
}