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

class TopologyAggregator {
  
  def aggregate(input:RDD[(ArrayBuffer[(String, String, String)], ArrayBuffer[(String, String, String, String, String)],String)]): String = {
    
    //vertices : (ip,id,type)
    //edges : (inV,outV,_id,type,responseTime)
    val verticesData = input.flatMap(_._1).map( x => (x,1))
    val edgesData = input.flatMap(_._2).map( x => ((x._1,x._2,x._3,x._4),(x._5.toDouble,1)))
    val timeData = input.map( x => (x._3,x._3.hashCode())).sortBy( x => x._2).map(_._1).take(1)(0)
    val verticesCount = verticesData.reduceByKey(_+_).map(x => (x._1._1,x._1._2,x._1._3,x._2))
    val edgesCount = edgesData.reduceByKey((x,y) => {
      var count = x._1 + y._1
      var num = x._2 + y._2
      (count/num,num)
    }).map( x => (x._1._1,x._1._2,x._1._3,x._1._4,x._2._1,x._2._2))
    
    //verticesCount : (ip,id,type,label)
    //edgesCount : (inV,outV,type,responseTime/label,label)
    val vertices = verticesCount.collect().toBuffer
    val edges = edgesCount.collect().toBuffer
    val verticesJson = vertices.map( x => {
        (
          ("name" -> x._1) ~
          ("_id" -> x._2) ~
          ("_type" -> x._3) ~
          ("_label" -> x._4)
        )
      })
    val edgesJson = edges.map( x => {
        (
          ("_inV" -> x._1) ~
          ("_outV" -> x._2) ~
          ("_type" -> x._4) ~
          ("_id" -> x._3) ~
          ("_responseTime" -> x._5) ~
          ("_label" -> x._6)
        )
      })
    val jsonCombined = ("vertices" -> verticesJson) ~ ("edges" -> edgesJson)
    val jsonCompleted = ("graph" -> jsonCombined)
    val jsonSucc = ("timestamp" -> timeData) ~ jsonCompleted
    compact(render(jsonSucc))
  }
}