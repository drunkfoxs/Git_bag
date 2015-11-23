package spark.algorithm.nesting

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import spark.algorithm.nesting.utils.ESConnection

/**
 * Created by liziyao on 15/10/23.
 */
class ESReceiver () extends org.apache.spark.streaming.receiver.Receiver[String](StorageLevel.MEMORY_AND_DISK_SER_2){

  def onStart(){
    new Thread("elasticsearch recevier"){
      override def run() { receiver() }
    }.start()
  }

  def onStop(){
  }

  private def receiver(){

    val esConn = new ESConnection()
    val recordArray = esConn.searchByWindow("logstash-*", 5)
    store(recordArray)
    esConn.close()
    restart("Trying to connect again")
  }
}
