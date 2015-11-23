package spark.algorithm.nesting

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat

import spark.math.MinimalRank

/*
 * The nesting algorithm takes an neutral inputs format,
 * doesn't depend on the upstream packet processor.
 */

class TopologyConstructor extends java.io.Serializable {

  def timeStringParser(timestamp: String) = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val time = formatter.parse(timestamp)
    time.getTime()
  }

  def construct(input: RDD[String], sc: org.apache.spark.SparkContext): RDD[ArrayBuffer[String]] = {
    val rddRecords = input.map(x => parse(x))
    val arrayRecords = rddRecords.collect()

    /*
     * Select all next hop candidates based on the IP address,
     * parent.server_ip == child.client_ip.
     * For A->B->[C,D,...], use record (A->B)'s uuid and server_ip as the key of candidates group
     */
    val nextHopCandidates = rddRecords.flatMap(x => {
      var arrBuff = new ArrayBuffer[(JValue, JValue)]()
      for (j <- 0 to arrayRecords.size - 1) {
        val server_ip = compact(x \ "server_ip")
        val client_ip = compact(x \ "client_ip")
        if (server_ip == compact(arrayRecords(j) \ "client_ip"))
          arrBuff = arrBuff :+(x, arrayRecords(j))
        else if (client_ip == compact(arrayRecords(j) \ "server_ip"))
          arrBuff = arrBuff :+(arrayRecords(j), x)
      }
      if (arrBuff.size == 0)
        arrBuff = arrBuff :+(x, x)
      arrBuff
    }).distinct().map(x => (compact(x._1 \ "server_ip") + "_" + compact(x._1 \ "uuid"), x))


    /*
     * From all next hop candidates select the most-likely one,
     * based on nesting policy: MIN(parent.res_out - child.res_in).
     * For example, A->B->[C,D,...] => ((A->B), (B->C)) and ((B->D), (B->D)) for next step
     */
    val allTwoHops = nextHopCandidates.groupByKey().flatMap(x => {
      val rank = new MinimalRank()
      val candidatesWithScore = x._2.toList.map(y => {
        val parent_res_out = timeStringParser(compact(y._1 \ "res_out").replaceAll("\"", ""))
        val child_res_in = timeStringParser(compact(y._2 \ "res_in").replaceAll("\"", ""))
        val scoreResponse = parent_res_out - child_res_in
        if (scoreResponse >= 0)
          rank.set(scoreResponse)
        (y, scoreResponse)
      })
      val highestProbabilityScore = rank.highestProbability()
      val candidates = candidatesWithScore.filter(x => x._2 == highestProbabilityScore)

      //FIXME: Only get the first record from ones equally have the highestProbabilityScore
      val candidateSelected = candidates.take(1).map(x => x._1)
      val restCandidates = candidates.toBuffer //Rest unselected pairs with equal highestProbabilityScore
      if (restCandidates.size > 0)
        restCandidates.remove(0)
      val unpaired = candidatesWithScore.filter(x => x._2 != highestProbabilityScore) union restCandidates.toList
      val unpairedCandidates = unpaired.map(x => (x._1._2, x._1._2))
      candidateSelected union unpairedCandidates
    })

    /*
     * Following the above step, any single-hops like ((B->D), (B->D)) are changed to (B->D)
     */
    val allHops = allTwoHops.distinct().map(x => {
      var arrBuff = new ArrayBuffer[String]()
      val first_hop = compact(render(x._1))
      val second_hop = compact(render(x._2))
      if (first_hop == second_hop)
        arrBuff += first_hop
      else
        arrBuff +=(first_hop, second_hop)
      arrBuff
    })

    // Single Hops
    // FIXME: Need a way to filter out single-hops contained in multi-hops
    val singleHops_per = allHops.filter(x => x.size == 1)

    /*
     * In this step, two-hop topologies are chained together to form a multi-hop topology.
     * If not chained, the two-hop form is kept
     */
    def checkContaining(arg1: ArrayBuffer[ArrayBuffer[String]], arg2: Set[String]): ArrayBuffer[ArrayBuffer[String]] = {
      var judge = new ArrayBuffer[ArrayBuffer[String]]()
      for (i <- 0 to arg1.size - 1) {
        var arrBuff = new ArrayBuffer[String]()
        val set1 = arg1(i).toSet
        if ((set1 & arg2).size == 0) {
          arrBuff ++= set1
          judge += arrBuff
        }
      }
      judge
    }

    def concatenateHops(multiHopCandidates: ArrayBuffer[ArrayBuffer[String]], twoAndMultiHops: ArrayBuffer[ArrayBuffer[String]]): ArrayBuffer[ArrayBuffer[String]] = {
      var newdata = new ArrayBuffer[ArrayBuffer[String]]()
      val left = multiHopCandidates.map(x => x.head).toSet
      val right = multiHopCandidates.map(x => x.last).toSet
      val intersectData = left & right
      val sd = checkContaining(multiHopCandidates, intersectData)
      twoAndMultiHops ++= sd
      if (intersectData.size == 0)
        return twoAndMultiHops
      else {
        for (i <- 0 to intersectData.size - 1) {
          var tmp = new ArrayBuffer[String]()
          for (j <- 0 to multiHopCandidates.size - 1) {
            if (multiHopCandidates(j).last == intersectData.toList(i)) {
              tmp = multiHopCandidates(j) ++: tmp // add data(j) at head
            }
            else if (multiHopCandidates(j).head == intersectData.toList(i)) {
              tmp ++= multiHopCandidates(j)
            }
          }
          newdata += tmp.distinct
        }
        concatenateHops(newdata, twoAndMultiHops)
      }
    }

    val twoHops = allHops.filter(x => x.size > 1).collect.toBuffer
    var multiHopCandidates = new ArrayBuffer[ArrayBuffer[String]]()
    multiHopCandidates ++= twoHops
    var hopsBuffer = new ArrayBuffer[ArrayBuffer[String]]()
    val multiHops = sc.parallelize(concatenateHops(multiHopCandidates, hopsBuffer))

    // FIXME: Need a way to filter out single-hops contained in multi-hops
    //multiHops union singleHops
    val copyMultiHops = multiHops.flatMap(x => x).map(x => (x, 1))
    val copySingleHops = singleHops_per.flatMap(x => x).map(x => (x, 1))

    val maxJoinIt = copySingleHops.leftOuterJoin(copyMultiHops)
    val singleHops = maxJoinIt.filter(x => x._2._2 == None).map(x => {
      var box = new ArrayBuffer[String]()
      box += x._1
      box
    })
    multiHops union singleHops
  }
}
