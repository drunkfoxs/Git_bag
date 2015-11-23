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

class GraphSONFormatter() extends java.io.Serializable {

  //Format output to next step aggregation
  def format(input: RDD[ArrayBuffer[String]]) = {

    def timeStringParser(timestamp: String) = {
      val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      val time = formatter.parse(timestamp)
      time.getTime()
    }

    val edges = input.map(x => {

      //vertices : (name,_id,_type,_label)
      var vertices = new ArrayBuffer[(String, String, String)]()

      //edges : (_inV,_outV,_type,_id,_response-time,_label)
      var edges = new ArrayBuffer[(String, String, String, String, String)]()

      def changeIP(ip: String) = {
        val ipArray = ip.split("\\.")
        var result = new ArrayBuffer[String]()
        for (i <- 0 to ipArray.size - 1) {
          var tmp = ipArray(i)
          for (j <- 1 to 3 - ipArray(i).size)
            tmp = "0" + tmp
          result = result :+ tmp
        }
        result.mkString("")
      }

      for (i <- 0 to x.size - 1) {
        val enums = x(i)
        val enum = parse(enums)
        val name_1 = compact(enum \ "client_ip")
        val name_2 = compact(enum \ "server_ip")
        val id_1 = changeIP(name_1)
        val id_2 = changeIP(name_2)
        val type_1, type_2 = "vertex"
        val node_1 = (name_1, id_1, type_1)
        val node_2 = (name_2, id_2, type_2)
        vertices += (node_1, node_2)

        val _inV = compact(enum \ "server_ip")
        val _outV = compact(enum \ "client_ip")
        val _id = changeIP(_outV).replaceAll("\"", "") + changeIP(_inV).replaceAll("\"", "")
        //          val _id = changeIP(_outV)+changeIP(_inV)
        val _type = "edge"
        val response_time = timeStringParser(compact(enum \ "res_in").replaceAll("\"", "")) - timeStringParser(compact(enum \ "req_out").replaceAll("\"", ""))
        val consEdge = (_inV, _outV, _id, _type, response_time + "")
        edges += consEdge
      }
      val timestamp = compact(parse(x(0)) \ "req_out")
      (vertices, edges, timestamp)
    })
    edges
  }

  //Format to string for persistence
  def formatToString(input: RDD[ArrayBuffer[String]]) = {
    val result1 = format(input)
    val result2 = result1.map(x => {
      val vertices = x._1
      val edges = x._2
      val timestamp = x._3
      val verticesJson = vertices.distinct.map(x => {
        (
          ("name" -> x._1) ~
          ("_id" -> x._2) ~
          ("_type" -> x._3) ~
          ("_label" -> "null"))
      })
      val edgesJson = edges.zipWithIndex.map(x => {
        (
          ("_inV" -> x._1._1) ~
          ("_outV" -> x._1._2) ~
          ("_type" -> x._1._4) ~
          ("_id" -> x._1._3) ~
          ("_responseTime" -> x._1._5) ~
          ("_label" -> "null"))
      })
      val jsonCombined = ("vertices" -> verticesJson) ~ ("edges" -> edgesJson)
      val jsonCompleted = ("graph" -> jsonCombined)
      val jsonSucc = ("timestamp" -> x._3) ~ jsonCompleted
      compact(render(jsonSucc))
    })
    result2
  }
}