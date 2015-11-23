package spark.algorithm.nesting.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import spark.algorithm.nesting._
import java.io.File
import java.io.PrintWriter
import scala.io.Source

object TopologyConstructorTest {
  def main(args: Array[String]) {
    
    val master = "local[2]"
    val conf = new SparkConf().setMaster(master).setAppName("ConstructTopologyTest")
    val sc = new SparkContext(conf)
    
    val data = sc.textFile("data/TopologyConstructorTestData.json", 2)
    val con = new TopologyConstructor()
    val result = con.construct(data, sc)
    
    val writer = new PrintWriter(new File("data/GraphSONFormatterTestData.json"))
    result.collect().foreach(x=>writer.println(x))
    writer.close()
  }
}