package spark.pcap.processor

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.slf4j.LoggerFactory

class ExampleReader {      
  val master = "local[2]"  
    
  val dataDir = "data/README.md" 
  val conf = new SparkConf().setAppName("ExampleReader").setMaster(master)
  val sc = new SparkContext(conf)
  val logger = LoggerFactory.getLogger(getClass())
    
  def exampleProcessing() {
    logger.info("Start reading the example README.md ...")     
    val logData = sc.textFile(dataDir, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    logger.info("Lines with a: %s, Lines with b: %s".format(numAs, numBs)) 
  }
}