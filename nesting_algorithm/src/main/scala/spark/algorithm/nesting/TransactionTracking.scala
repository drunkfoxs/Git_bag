package spark.algorithm.nesting

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import kafka.serializer.StringDecoder
import spark.algorithm.nesting.utils.ConfigUtils
import spark.algorithm.nesting.utils.ESConnection

object TransactionTracking {

  //Streaming job configuration
  val MASTER = "local[2]"
  val STREAMING_JOB_NAME = "TransactionTracking"
  val MICRO_BATCH_WINDOW = new Duration(60000) //60 seconds, 1 minute

  //Get values from the property files
  val kafkaPropFile = "/conf/kafka.properties"
  val kafkaProp = ConfigUtils.getConfig(kafkaPropFile)
  val k_isBroker = kafkaProp.get("IS_BROKER").get
  val k_broker = kafkaProp.getOrElse("IPADDR", "localhost:9092")
  val k_topic = kafkaProp.getOrElse("TOPIC", "packetbeat")

  val esPropFile = "/conf/elasticsearch.properties"
  val esProp = ConfigUtils.getConfig(esPropFile)
  val e_isBroker = esProp.get("IS_BROKER").get
  val e_outputIndexName = esProp.getOrElse("OUTPUT_INDEX_NAME", "transaction")
  val e_outputTypeNameSingle = esProp.getOrElse("OUTPUT_TYPE_NAME_SINGLE", "single")
  val e_outputTypeNameAggreg = esProp.getOrElse("OUTPUT_TYPE_NAME_AGGREG", "aggregated")
  val e_inputIndexName = esProp.getOrElse("INPUT_INDEX_NAME", "logstash-*")
  val e_inputLoadInterval = esProp.getOrElse("INPUT_LOAD_INTERVAL", "1").toInt

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster(MASTER).setAppName(STREAMING_JOB_NAME)
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, MICRO_BATCH_WINDOW)

    if (k_isBroker.equals("true")) {
      val topicSet = Set[String](k_topic)
      val kafkaParams = Map("metadata.broker.list" -> k_broker)
      val records = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet).map(_._2)
      records.foreachRDD(inputRecords => process(inputRecords))
    }
    else if (e_isBroker.equals("true")) {
      val esConn = new ESConnection()
      //      val recordArray = esConn.searchByWindow(e_inputIndexName, e_inputLoadInterval)
      val recordArray = esConn.searchByWindow(e_inputIndexName, 5)
      val recordRDD = sc.parallelize(recordArray)
      process(recordRDD)
    }

    def process(inputRecords: RDD[String]) {
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
        esIndexer.index(e_outputIndexName, e_outputTypeNameSingle, f.replaceAll("\\\\", "").replaceAll("\"\"", "\""))
      })
      //Aggregated
      esIndexer.index(e_outputIndexName, e_outputTypeNameAggreg, aggregatedTransaction.replaceAll("\\\\", "").replaceAll("\"\"", "\""))
      esIndexer.close()
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

