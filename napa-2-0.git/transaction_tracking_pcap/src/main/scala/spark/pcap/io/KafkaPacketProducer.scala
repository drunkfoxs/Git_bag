package spark.pcap.io

import java.util.HashMap
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}

class KafkaPacketProducer(props: HashMap[String, Object], topic : String) extends java.io.Serializable {
  
  val producer = new KafkaProducer[String, String](props)
  
  def sendMsg(content : String) {
    
    val message = new ProducerRecord[String, String](topic, null, content)
    
    producer.send(message)
    
  }
}