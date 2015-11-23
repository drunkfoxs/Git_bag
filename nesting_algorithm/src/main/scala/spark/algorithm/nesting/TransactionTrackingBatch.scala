package spark.algorithm.nesting

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import spark.algorithm.nesting.utils.ESConnection

object TransactionTrackingBatch {
  
  val MASTER = "local[2]"
  val e_outputIndexName = "test"
  val e_outputTypeNameSingle = "single"
  val e_outputTypeNameAggreg = "aggregated"
  
   def main(args: Array[String]) {
     
     val conf = new SparkConf().setMaster(MASTER).setAppName("TransactionTracking")
     val sc = new SparkContext(conf)
     
     val inputRecords = sc.textFile("data/PBProcessorTestData.json",2)     
     val algorithmInputs = sc.textFile("data/TopologyConstructorTestData.json",2)
     
     //Process PB records to nesting algorithm inputs
     val pbProcessor = new PBProcessor()
     val algorithmInput = pbProcessor.process(inputRecords)
     
     //Nesting algorithm
     val topologyConstructor = new TopologyConstructor()
     val algorithmOutput = topologyConstructor.construct(algorithmInput, sc)
     
     //Format to GraphSON
     val gFormatter = new GraphSONFormatter()
     val singleTransactionRDD = gFormatter.format(algorithmOutput)
     val singleTransactionSTR = gFormatter.formatToString(algorithmOutput)
     
     //Aggregated topology based on GraphSON input
     val topologyAggregator = new TopologyAggregator()
     val aggregatedTransaction = topologyAggregator.aggregate(singleTransactionRDD) 
     
     //Output to Elasticsearch     
     val esIndexer = new ESConnection()
     //Single
     singleTransactionSTR.collect().foreach(f => {
       esIndexer.index(e_outputIndexName, e_outputTypeNameSingle, f.replaceAll("\\\\","").replaceAll("\"\"","\""))
     })
     //Aggregated
     esIndexer.index(e_outputIndexName, e_outputTypeNameAggreg, aggregatedTransaction.replaceAll("\\\\","").replaceAll("\"\"","\""))
     esIndexer.close()
   }
}