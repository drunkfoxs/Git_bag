package spark.topology.objects

import scala.collection.mutable.HashMap
import spark.topology.processors.FlowProcessor

class FlowObject extends java.io.Serializable {
  val HTTP_REQUEST_INDEX = "Request";
  val HTTP_RESPONSE_INDEX = "Response";

  /*
   * Construct the flowObject from json string
   */

  def buildFlowObject(line: String): SplitFlow = {
    val flowProcessor = new FlowProcessor()
    val splitFlow: SplitFlow = flowProcessor.parseFlow(line)
    splitFlow
  }

  /*
   * Construct key to reduce duplicate packets
   */
  def buildFlowWithReduceDuplicateKey(splitFlow: SplitFlow): HashMap[String, SplitFlow] = {
    var flowWithReduceDuplicateKey = new HashMap[String, SplitFlow]()
    var reduceDuplicateKey = splitFlow.getSplitFlowAttributes().toString()
    flowWithReduceDuplicateKey.put(reduceDuplicateKey, splitFlow)
    flowWithReduceDuplicateKey
  }

  /*
   * Construct key to group packets with the same 
   * For Http Request: <src_ip=$src_ip, src_port=$src_port, dst_ip=$dst_ip, dst_port=$dst_port, time=$tsval>
   * For Http Response: <src_ip=$dst_ip, src_port=$dst_port, dst_ip=$src_ip, dst_port=$src_port, time=$tsecr>
   */
  def buildFlowWithGroupKey(splitFlow: SplitFlow): HashMap[String, SplitFlow] = {
    var src = ""
    var srcPort = -1
    var dst = ""
    var dstPort = -1
    var protocol = ""
    var timestamp = -1l

    var flowWithGroupKey = new HashMap[String, SplitFlow]()

    protocol = splitFlow.getSplitFlowAttributes().getProtocol();
    if (splitFlow.getSplitFlowAttributes().getHttpType() == HTTP_REQUEST_INDEX) {
      src = splitFlow.getSplitFlowAttributes().getSrc();
      srcPort = splitFlow.getSplitFlowAttributes().getSrcPort();
      dst = splitFlow.getSplitFlowAttributes().getDst();
      dstPort = splitFlow.getSplitFlowAttributes().getDstPort();
      timestamp = splitFlow.getSplitFlowAttributes().getTsVal();
    } else if (splitFlow.getSplitFlowAttributes().getHttpType() == HTTP_RESPONSE_INDEX) {
      dst = splitFlow.getSplitFlowAttributes().getSrc();
      dstPort = splitFlow.getSplitFlowAttributes().getSrcPort();
      src = splitFlow.getSplitFlowAttributes().getDst();
      srcPort = splitFlow.getSplitFlowAttributes().getDstPort();
      timestamp = splitFlow.getSplitFlowAttributes().getTsEcr();
    }

    var groupID = new StringBuilder().append(src).append("_")
      .append(srcPort).append("_").append(dst).append("_")
      .append(dstPort).append("_").append(protocol).append("_")
      .append(timestamp).toString();

    flowWithGroupKey.put(groupID, splitFlow)
    return flowWithGroupKey
  }

  /*
   * Construct key to discover "InTime" for Http request / response
   * For Http Request / Response: <src_ip=$src_ip, src_port=$src_port, dst_ip=$dst_ip, dst_port=$dst_port, time=$tsval>
   * For ACK: <src_ip=$dst_ip, src_port=$dst_port, dst_ip=$src_ip, dst_port=$src_port, time=$tsecr>
   */

  def buildFlowWithInTimeKey(splitFlow: SplitFlow): HashMap[String, SplitFlow] = {
    var time = -1l
    var src = ""
    var srcPort = -1
    var dst = ""
    var dstPort = -1
    var flowWithInTimeKey = new HashMap[String, SplitFlow]()

    if (splitFlow.getSplitFlowAttributes().getHttpType() == HTTP_REQUEST_INDEX || splitFlow.getSplitFlowAttributes().getHttpType() == HTTP_RESPONSE_INDEX) {
      time = splitFlow.getSplitFlowAttributes.getTsVal()
      src = splitFlow.getSplitFlowAttributes().getSrc();
      srcPort = splitFlow.getSplitFlowAttributes().getSrcPort();
      dst = splitFlow.getSplitFlowAttributes().getDst();
      dstPort = splitFlow.getSplitFlowAttributes().getDstPort();
    } else if (splitFlow.getSplitFlowAttributes().getIsAck()) {
      dst = splitFlow.getSplitFlowAttributes().getSrc();
      dstPort = splitFlow.getSplitFlowAttributes().getSrcPort();
      src = splitFlow.getSplitFlowAttributes().getDst();
      srcPort = splitFlow.getSplitFlowAttributes().getDstPort();
      time = splitFlow.getSplitFlowAttributes().getTsEcr()
    }
    var inTimeID = new StringBuilder().append(src).append("_")
      .append(srcPort).append("_").append(dst).append("_")
      .append(dstPort).append("_").append(time).toString();
    
    flowWithInTimeKey.put(inTimeID, splitFlow)
    return flowWithInTimeKey
  }
  
  /*
   * Discover ACKs with the same key shown below. For the purpose of extracting the smallest tsval within the same key
   * For ACK: <src_ip=$dst_ip, src_port=$dst_port, dst_ip=$src_ip, dst_port=$src_port, time=$tsecr>
   * For none ACK: <noneACK>
   */
  
  def buildAckWithInTimeKey(splitFlow: SplitFlow): HashMap[String, SplitFlow] = {
    var time = -1l
    var src = ""
    var srcPort = -1
    var dst = ""
    var dstPort = -1    
    var ackWithInTimeKey = new HashMap[String, SplitFlow]()
    
    if (splitFlow.getSplitFlowAttributes().getIsAck()){
      dst = splitFlow.getSplitFlowAttributes().getSrc();
      dstPort = splitFlow.getSplitFlowAttributes().getSrcPort();
      src = splitFlow.getSplitFlowAttributes().getDst();
      srcPort = splitFlow.getSplitFlowAttributes().getDstPort();
      time = splitFlow.getSplitFlowAttributes().getTsEcr()   
      var inTimeID = new StringBuilder().append(src).append("_")
      .append(srcPort).append("_").append(dst).append("_")
      .append(dstPort).append("_").append(time).toString()
      ackWithInTimeKey.put(inTimeID, splitFlow) 
    } else {        
      var inTimeID = "noneACK"  
      ackWithInTimeKey.put(inTimeID, splitFlow) 
    }            
    return ackWithInTimeKey
  }
}