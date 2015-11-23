package spark.pcap.run

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spark.pcap.processor.ExampleReader
import spark.pcap.processor.PcapProcessor
import scala.collection.mutable.ArrayBuffer

object PacketPathDiscover {
  val logger = LoggerFactory.getLogger(getClass())

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
    	
      logger.error("usage: PacketPathDiscover <captureType(1-file, 2-live, 3-kafka)> <pcapfile|interface|kafkahost>");
      System.exit(-1);

    }
    logger.info("Pcap source type:" + args(0) + ", source: " + args(1))
    val PcapProcessor = new PcapProcessor()
    PcapProcessor.packetProcessing(Integer.parseInt(args(0)), args(1))

  }
}