package spark.algorithm.nesting.utils

import java.util.Properties
import scala.collection.JavaConversions.propertiesAsScalaMap
import scala.collection.mutable.Map

object ConfigUtils {

  def getConfig(path:String) : Map[String,String] = {
    val prop = new Properties()
    val inputStream = getClass().getResourceAsStream(path)
    try{
    prop.load(inputStream)
    propertiesAsScalaMap(prop)
    }
    finally inputStream.close()
  }
  
  //Test
  def main (args:Array[String]){
    val path = "/conf/elasticsearch.properties"
    val prop = getConfig(path)
    val ip = prop.get("IPADDR")
    val port = prop.get("PORT")
    println(ip + "\t" +port)
  }
}