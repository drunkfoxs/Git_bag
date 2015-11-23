package spark.algorithm.nesting

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import spark.math.MinimalRank
import java.text.SimpleDateFormat

object ConstructTopology {
  def topologyProduct(input:List[org.json4s.JsonAST.JValue]) {
    // val master = "local[2]"
    // val file = "data/change"
    // val conf = new SparkConf().setMaster(master).setAppName("Simple Application")
    // val sc = new SparkContext(conf)
    // val sample = sc.textFile(file, 2).map(_.split(",")).cache()

    val data = sc.parallelize(input).map( x => {

        val client_ip = compact(x \ "client_ip")
        //注意  返回值带双引号


    })



    val data_list = sample.toArray()

    def timeParse(time:String) = {

     val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
     val date = formatter.parse(time)
     date.getTime()
    }

    def getDate(arg1:String,arg2:String) = {
      val result1 = timeParse(arg1)
      val result2 = timeParse(arg2)
      result1 - result2
    }

    def checkTrue(arg:Array[String],arg2:Array[String]) = {
      val per_time = getDate(arg(5),arg(6))
      val next_time = getDate(arg2(8),arg2(7))
      val per_cli_out = timeParse(arg(6))
      val per_cli_in = timeParse(arg(5))
      val next_cli_out = timeParse(arg2(6))
      val next_cli_in = timeParse(arg2(5))

      if ( per_time > next_time )
       true
      else false
    }

    val result = sample.flatMap( x => {
      var arrBuff = new ArrayBuffer[(Array[String],Array[String])]()

        for( j <- 0 to data_list.size-1){
          if( x(3) == data_list(j)(1) )
            arrBuff = arrBuff :+ (x,data_list(j))
        }

      arrBuff
    }).map( x => (x._1(3),x))

    val edgeGroup = result.groupByKey().flatMap( x => {
      val rank = new MinimalRank()
      val edges = x._2.toList.map( x => {
        val per_server_out = timeParse(x._1(8))
        val next_cli_in = timeParse(x._2(5))
        val scoreResponse = per_server_out - next_cli_in
        println("debuggggggggggggggg---"+x._1(0)+"-"+x._2(0)+"-----!"+scoreResponse)
        if(scoreResponse >= 0)
        rank.set(scoreResponse)
        (x,scoreResponse)
      })
      val highestProbabilityScore = rank.highestProbability()
      println("********************"+highestProbabilityScore)
      val trueEdge = edges.filter( x => x._2 == highestProbabilityScore).map( x => x._1)
      trueEdge
    }).map( x => (x._1(0),x._2(0)))



    edgeGroup.zipWithIndex().collect().foreach(f => println(f))

//    edgeGroup.collect().foreach(f => println(f)).


    //TODO: add construction logic
    /*
     * Expect output:[(001,002),(002,005)]
     */

  }
}
