package test

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.settings.ImmutableSettings

/**
 * Created by liziyao on 15/9/14.
 */
object TestJson {
  def main(args: Array[String]) {
    val a = "{\"uuid\": \"001\",\"client_ip\": \"9.12.248.10\",\"client_port\": 54161,\"server_ip\": \"9.12.248.11\",\"server_port\": 8080,\"res_in\": \"2015-08-27T12:19:02.000Z\",\"req_out\": \"2015-08-27T12:19:01.000Z\",\"req_in\": \"2015-08-27T12:19:01.010Z\",\"res_out\": \"2015-08-27T12:19:01.990Z\"}"

    val setting = ImmutableSettings.settingsBuilder().put("client.transport.sniff",true).build()
    val client = new TransportClient()
    val transportAddress = new InetSocketTransportAddress("localhost",9300)
    client.addTransportAddress(transportAddress)

    val response = client.prepareIndex("demo","single_transaction").setSource(a).execute().actionGet()

    println(response.isCreated)


  }
}
