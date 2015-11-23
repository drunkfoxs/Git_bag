package spark.algorithm.nesting.utils

import java.net.{HttpURLConnection, URL, URLConnection}
import java.text.SimpleDateFormat
import java.util.Date

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.index.query.QueryBuilders
import scala.collection.mutable.ArrayBuffer
import scalaj.http.Http
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class ESConnection {

  //Get values from the property file
  val propFile = "/conf/elasticsearch.properties"
  val monitorFile = "/conf/monitors.properties"
  val prop = ConfigUtils.getConfig(propFile)
  val propMon = ConfigUtils.getConfig(monitorFile)
  val ipAddress = prop.getOrElse("IPADDR", "localhost")
  val port = prop.getOrElse("PORT", "9300").toInt

  //Initiate elastic client
  val setting = ImmutableSettings.settingsBuilder()
  setting.put("client.transport.sniff", true).build()
  val client = new TransportClient(setting)
  val transportAddress = new InetSocketTransportAddress(ipAddress, port)
  client.addTransportAddress(transportAddress)

  //Index one record
  def index(indexName: String, typeName: String, record: String) {
    val response = client.prepareIndex(indexName, typeName)
      .setSource(record)
      .execute()
      .actionGet()
  }

  def timeStringParser(timestamp: String) = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmmSS")
    val time = formatter.parse(timestamp)
    time.getTime()
  }

  def calculate_time_delta_minutes(old_time: String, new_time: String): Int = {
    val oldTime = timeStringParser(old_time)
    val newTime = timeStringParser(new_time)
    return ((newTime - oldTime) / 1000 / 60).toInt
  }

  def searchByWindow(): ArrayBuffer[String] = {
    val jsonStr = new ESTokens().getResult()
    val json = parse(jsonStr)
    val count = render(json \ "hits" \ "hits" \ "_source" \ "log").children
    var arrBuff = new ArrayBuffer[String]()
    count.foreach(x => {
      val log = x.values.toString
      arrBuff += log
    })
    arrBuff
  }

  //Search records in a time window (from interval minutes ago)
  def searchByWindow(indexName: String, interval: Int): ArrayBuffer[String] = {

    val rangeString = "now-" + interval.toString() + "m"

    //Get approximated max size first
    val sizeResp = client.prepareSearch(indexName)
      .setQuery(QueryBuilders.rangeQuery("timestamp").gte(rangeString).lte("now"))
      .execute()
      .actionGet()
    val maxSize = sizeResp.getHits().getTotalHits() * 1.2

    //Now get the real records
    val recordResp = client.prepareSearch(indexName)
      .setQuery(QueryBuilders.rangeQuery("timestamp").gte(rangeString).lte("now"))
      .setSize(maxSize.toInt)
      .execute()
      .actionGet()
    val count = recordResp.getHits().getTotalHits().toInt
    val hitsArray = recordResp.getHits()
    val strArray: ArrayBuffer[String] = new ArrayBuffer[String](count)
    for (i <- 0 to count - 1) {
      strArray(i) = hitsArray.getAt(i).sourceAsString()
    }

    return strArray
  }

  def close() {
    client.close()
  }
}

//Test
object ESConnectionTest {

  val esPropFile = "/conf/elasticsearch.properties"
  val esProp = ConfigUtils.getConfig(esPropFile)
  val indexName = esProp.getOrElse("INPUT_INDEX_NAME", "logstash-*")

  def main(args: Array[String]) {
    val esIndexer = new ESConnection()
    val records = esIndexer.searchByWindow(indexName, 1)
    for (currRcd <- records) {
      println(currRcd)
    }
  }
}