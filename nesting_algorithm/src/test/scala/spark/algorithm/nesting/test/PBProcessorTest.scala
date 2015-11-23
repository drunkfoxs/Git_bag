package spark.algorithm.nesting.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import spark.algorithm.nesting._
import java.io.File
import java.io.PrintWriter
import scala.io.Source

object PBProcessorTest {
  def main(args: Array[String]) {
    
    val master = "local[2]"
    val conf = new SparkConf().setMaster(master).setAppName("ConstructTopologyTest")
    val sc = new SparkContext(conf)
    
    val data = sc.textFile("data/PBProcessorTestData.json", 2)
    val pbp = new PBProcessor()
    val result = pbp.process(data)
    
    val writer = new PrintWriter(new File("data/TopologyConstructorTestData.json"))
    result.collect().foreach(x=> writer.println(x))
    writer.close()
  }
}