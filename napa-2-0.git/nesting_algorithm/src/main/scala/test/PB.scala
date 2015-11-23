package test

/**
 * Created by liziyao on 15/9/13.
 */

import java.text.SimpleDateFormat
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.ArrayBuffer

object PB {
  def main(args: Array[String]) {
    //val a = new Con()
//    a.ss = "liziyao"


    // TODO
    // parse json , give json to Con
//    println(a.ss)
    val a = "{\"bytes_in\":201,\"bytes_out\":0,\"client_ip\":\"9.12.248.100\",\"client_port\":54161,\"client_proc\":\"\",\"client_server\":\"\",\"count\":1,\"http\":{\"code\":200,\"content_length\":11230,\"phrase\":\"OK\"},\"ip\":\"172.17.0.210\",\"method\":\"GET\",\"params\":\"\",\"path\":\"/\",\"port\":8080,\"proc\":\"\",\"query\":\"GET /\",\"responsetime\":6,\"server\":\"\",\"shipper\":\"acmeair-test\",\"status\":\"OK\",\"timestamp\":\"2015-08-27T12:19:20.745Z\",\"type\":\"http\"}"
    val b = "{\"bytes_in\":250,\"bytes_out\":2859,\"client_ip\":\"9.12.248.100\",\"client_port\":54162,\"client_proc\":\"\",\"client_server\":\"\",\"count\":1,\"http\":{\"code\":401,\"content_length\":2474,\"phrase\":\"Unauthorized\"},\"ip\":\"172.17.0.210\",\"method\":\"GET\",\"params\":\"\",\"path\":\"/manager/status\",\"port\":8080,\"proc\":\"\",\"query\":\"GET /manager/status\",\"responsetime\":4,\"server\":\"\",\"shipper\":\"acmeair-test\",\"status\":\"Error\",\"timestamp\":\"2015-08-27T12:19:26.199Z\",\"type\":\"http\"}"
    val c = "{\"bytes_in\":201,\"bytes_out\":0,\"client_ip\":\"172.17.1.44\",\"client_port\":54161,\"client_proc\":\"\",\"client_server\":\"\",\"count\":1,\"http\":{\"code\":200,\"content_length\":11230,\"phrase\":\"OK\"},\"ip\":\"9.12.248.91\",\"method\":\"GET\",\"params\":\"\",\"path\":\"/\",\"port\":8080,\"proc\":\"\",\"query\":\"GET /\",\"responsetime\":106,\"server\":\"\",\"shipper\":\"acmeair-test2\",\"status\":\"OK\",\"timestamp\":\"2015-08-27T12:19:20.766Z\",\"type\":\"http\"}"
    val d = "{\"bytes_in\":250,\"bytes_out\":2859,\"client_ip\":\"172.17.1.44\",\"client_port\":54162,\"client_proc\":\"\",\"client_server\":\"\",\"count\":1,\"http\":{\"code\":401,\"content_length\":2474,\"phrase\":\"Unauthorized\"},\"ip\":\"9.12.248.91\",\"method\":\"GET\",\"params\":\"\",\"path\":\"/manager/status\",\"port\":8080,\"proc\":\"\",\"query\":\"GET /manager/status\",\"responsetime\":104,\"server\":\"\",\"shipper\":\"acmeair-test2\",\"status\":\"Error\",\"timestamp\":\"2015-08-27T12:19:26.219Z\",\"type\":\"http\"}"
    val pb_file = List[String](a,b,c,d)
    val result = pb_file.map( x => parse(x))
    var edgeBuffer = new ArrayBuffer[(String, String, String, String, Long, Long, Long, Long)]()

    def timeParse(time:String) = {
      val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      val date = formatter.parse(time)
      date.getTime()
    }

    for( i <- 0 to result.size-1){
      for( j <- 0 to result.size-1 ){

        if( compact(result(i) \ "bytes_in") ==  compact(result(j) \ "bytes_in")
         && compact(result(i) \ "bytes_out") == compact(result(j) \ "bytes_out")
         && compact(result(i) \ "client_ip") == compact(result(j) \ "client_ip")
         && compact(result(i) \ "ip") == compact(result(j) \ "ip")
         && compact(result(i) \ "client_port") == compact(result(j) \ "client_port")
         && compact(result(i) \ "port") == compact(result(j) \ "port")
         && compact(result(i) \ "timestamp") < compact(result(j) \ "timestamp")
        ){
          //TODO :two records A and B can be paired together!!
          val client_ip = compact(result(i) \ "ip").replaceAll("\"","")
          val client_port = compact(result(i) \ "port").replaceAll("\"","")
          val server_ip = compact(result(j) \ "ip").replaceAll("\"","")
          val server_port = compact(result(j) \ "port").replaceAll("\"","")
          val res_in = timeParse(compact(result(i) \ "timestamp").replaceAll("\"","")) + compact(result(i) \ "responsetime").replaceAll("\"","").toLong
          val req_out = timeParse(compact(result(i) \ "timestamp").replaceAll("\"",""))
          val req_in = timeParse(compact(result(j) \ "timestamp").replaceAll("\"",""))
          val res_out = timeParse(compact(result(j) \ "timestamp").replaceAll("\"","")) + compact(result(j) \ "responsetime").replaceAll("\"","").toLong

          val enumsOfEdge = (client_ip,client_port,server_ip,server_port,res_in,req_out,req_in,res_out)
          edgeBuffer = edgeBuffer :+ enumsOfEdge
        }

      }
    }

    val paires = edgeBuffer.zipWithIndex.map( x => {
   //(1,(client_ip.....res_out))
//      {
//        (
//          ("_id" -> node._id) ~
//            ("_type" -> node._type) ~
//            ("name" -> node.ip) ~
//            ("ip" -> node.ip))
//      }

      (
        ("uuid" -> x._2) ~
          ("client_ip" -> x._1._1) ~
          ("client_port" -> x._1._2) ~
          ("server_ip" -> x._1._3) ~
          ("server_port" -> x._1._4) ~
          ("res_in" -> x._1._5) ~
          ("req_out" -> x._1._6) ~
          ("req_in" -> x._1._7) ~
          ("res_out" -> x._1._8)
        )
    })

    val pairesJson = compact(render(paires))
    //[{"uuid":0,"client_ip":"172.17.0.210","client_port":"8080","server_ip":"172.17.0.210","server_port":"8080","res_in":1440649160751,"req_out":1440649160745,"req_in":1440649166199,"res_out":1440649166203},{"uuid":1,"client_ip":"9.12.248.91","client_port":"8080","server_ip":"9.12.248.91","server_port":"8080","res_in":1440649160872,"req_out":1440649160766,"req_in":1440649166219,"res_out":1440649166323}]









  }
}
