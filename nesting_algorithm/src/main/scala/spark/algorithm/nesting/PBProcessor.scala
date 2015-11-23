package spark.algorithm.nesting

import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import java.util.UUID
import java.text.SimpleDateFormat

/*
 * PacketBeat Processor, transform PB outputs to nesting algorithm inputs
 */

class PBProcessor extends java.io.Serializable{
    
  val uuid = UUID.randomUUID().toString()
  
  def process(input:RDD[String]):RDD[String] = {

    val rddPBRecords = input.map( x => parse(x)).cache()
    val arrayPBRecords = rddPBRecords.collect()

    def timeStringParser(timestamp:String) = {
      val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      val time = formatter.parse(timestamp)
      time.getTime()
    }
    
    def timeLongParser(timestamp:Long) = {
      val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      formatter.format(timestamp)
    }
    
    /*
     * Pair up two PB records (A as client and B as server) if: 
     * A.bytes_in == B.bytes_in
     * A.bytes_out == B.bytes_out
     * A.client_ip == B.client_ip
     * A.ip == B.ip
     * A.client_port == B.client_port
     * A.port == B.port
     * A.direction == "out"
     * B.direction == "in"
     */
    val rawPairs = rddPBRecords.map( x => {
      var pairBuffer = new ArrayBuffer[(String, String, String, String, String, String, String, String)]()
      for( curr <- arrayPBRecords ){
        if( compact(curr \ "bytes_in") ==  compact(x \ "bytes_in")
         && compact(curr \ "bytes_out") == compact(x \ "bytes_out")
         && compact(curr \ "client_ip") == compact(x \ "client_ip")
         && compact(curr \ "ip") == compact(x \ "ip")
         && compact(curr \ "client_port") == compact(x \ "client_port")
         && compact(curr \ "port") == compact(x \ "port")
         && compact(curr \ "direction") == "\"out\""
         && compact(x \ "direction") == "\"in\""
        )
        {
          val client_ip = compact(curr \ "client_ip").replaceAll("\"","")
          val client_port = compact(curr \ "client_port").replaceAll("\"","")
          val server_ip = compact(x \ "ip").replaceAll("\"","")
          val server_port = compact(x \ "port").replaceAll("\"","")
          val res_in = timeLongParser(timeStringParser(compact(curr \ "timestamp").replaceAll("\"","")) + compact(curr \ "responsetime").replaceAll("\"","").toLong)
          val req_out = timeLongParser(timeStringParser(compact(curr \ "timestamp").replaceAll("\"","")))
          val req_in = timeLongParser(timeStringParser(compact(x \ "timestamp").replaceAll("\"","")))
          val res_out = timeLongParser(timeStringParser(compact(x \ "timestamp").replaceAll("\"","")) + compact(x \ "responsetime").replaceAll("\"","").toLong)
          
          val currPair = (client_ip,client_port,server_ip,server_port,res_in,req_out,req_in,res_out)
          pairBuffer = pairBuffer :+ currPair
        }
      }
      pairBuffer
    }).filter( x => x.size > 0).map( x => x(0))
    
    
    val formattedPairs = rawPairs.zipWithIndex.map( x => {
      (
          ("uuid" -> UUID.randomUUID().toString()) ~
          ("client_ip" -> x._1._1) ~
          ("client_port" -> x._1._2) ~
          ("server_ip" -> x._1._3) ~
          ("server_port" -> x._1._4) ~
          ("res_in" -> x._1._5) ~
          ("req_out" -> x._1._6) ~
          ("req_in" -> x._1._7) ~
          ("res_out" -> x._1._8)
        )
    }).map( x => compact(render(x)))

    formattedPairs
  }
}