package spark.topology.objects

import spark.pcap.util.HashCodeUtil

class SplitFlowAttributes extends java.io.Serializable {
  /*
   * Below are the values captured from the packet streams
   */
  var src = ""
  var srcPort = -1
  var dst = ""
  var dstPort = -1
  var protocol = ""
  var tsVal = -1l
  var tsEcr = -1l
  var httpUri = ""
  var httpMethod = ""
  var httpType = ""
  var ack = false

  /*
   * Below are the values after calculation
   */
  var rtt = -1l  // The Round Trip Time for the source node
  var rttDst = -1l  // The Round Trip Time for the dest node
  var startTime = -1l // The time when the first request leave its src node
  var endTime = -1l // The time when the final response arrive at its dest node
  var arriveTime = -1l // The time when the first request arrive at its dest node
  var leaveTime = -1l // The time when the final response leave its src node

  def getSrc(): String = {
    return this.src
  }
  def setSrc(n: String) {
    this.src = n
  }
  def getSrcPort(): Integer = {
    return this.srcPort
  }
  def setSrcPort(port: Integer) {
    this.srcPort = port
  }
  def getDst(): String = {
    return this.dst
  }
  def setDst(n: String) {
    this.dst = n
  }
  def getDstPort(): Integer = {
    return this.dstPort
  }
  def setDstPort(port: Integer) {
    this.dstPort = port
  }
  def getProtocol(): String = {
    return this.protocol
  }
  def setProtocol(n: String) {
    this.protocol = n
  }
  def getTsVal(): Long = {
    return this.tsVal
  }
  def setTsVal(timestamp: Long) {
    this.tsVal = timestamp
  }
  def getTsEcr(): Long = {
    return this.tsEcr
  }
  def setTsEcr(timestamp: Long) {
    this.tsEcr = timestamp
  }
  def setRTT(time: Long) {
    this.rtt = time
  }
  def getRTT(): Long = {
    return this.rtt
  }
  def setRTTDst(time: Long) {
    this.rttDst = time
  }
  def getRTTDst(): Long = {
    return this.rttDst
  }
  def setStartTime(time: Long) {
    this.startTime = time
  }
  def getStartTime(): Long = {
    return this.startTime
  }
  def setEndTime(time: Long) {
    this.endTime = time
  }
  def getEndTime(): Long = {
    return this.endTime
  }
  def setArriveTime(time: Long) {
    this.arriveTime = time
  }
  def getArriveTime(): Long = {
    return this.arriveTime
  }
  def setLeaveTime(time: Long) {
    this.leaveTime = time
  }
  def getLeaveTime(): Long = {
    return this.leaveTime
  }
  def getHttpUri(): String = {
    return this.httpUri
  }
  def setHttpUri(n: String) {
    this.httpUri = n
  }
  def getHttpMethod(): String = {
    return this.httpMethod
  }
  def setHttpMethod(n: String) {
    this.httpMethod = n
  }
  def getHttpType(): String = {
    return this.httpType
  }
  def setHttpType(n: String) {
    this.httpType = n
  }
  def getIsAck(): Boolean = {
    return this.ack
  }
  def setIsAck(n: Boolean) {
    this.ack = n
  }
  
  override def toString(): String = {
    return ("src = " + this.src + ", srcPort = " + this.srcPort + ", dst = " + this.dst
      + ", dstPort = " + this.dstPort + ", protocol = " + this.protocol + ", tsVal = "
      + this.tsVal + ", tsEcr = " + this.tsEcr + ", httpUri = " + this.httpUri
      + ", httpMethod = " + this.httpMethod + ", httpType = " + this.httpType
      + ", rtt = " + this.rtt + ", rttDst = " + this.rttDst + ", startTime = " + this.startTime 
      + ", endTime = " + this.endTime + ", arriveTime = " + this.arriveTime + ", leaveTime = " 
      + this.leaveTime + ", IsAck = " + this.ack)
  }
    def canEqual(other: Any): Boolean =
    other.isInstanceOf[SplitFlowAttributes]

  override def equals(other: Any): Boolean =
    other match {
      case that: SplitFlowAttributes =>
        (that canEqual this) &&
          this.getArriveTime() == that.getArriveTime() &&
          this.getEndTime() == that.getEndTime() &&
          this.getLeaveTime() == that.getLeaveTime() &&
          this.getStartTime() == that.getStartTime() &&
          this.getSrc() == that.getSrc() &&
          this.getSrcPort() == that.getSrcPort() &&
          this.getDst() == that.getDst() &&
          this.getDstPort() == that.getDstPort() &&
          this.getHttpUri()== that.getHttpUri() &&
          this.getHttpMethod() == that.getHttpMethod() &&
          this.getIsAck() == that.getIsAck()
      case _ => false
    }

  override def hashCode: Int = {
    var result = HashCodeUtil.SEED;
    
    result = HashCodeUtil.hash(result, getArriveTime());
    result = HashCodeUtil.hash(result, getEndTime());
    result = HashCodeUtil.hash(result, getLeaveTime);
    result = HashCodeUtil.hash(result, getStartTime());
    result = HashCodeUtil.hash(result, getSrc());
    result = HashCodeUtil.hash(result, getSrcPort());
    result = HashCodeUtil.hash(result, getDstPort());
    result = HashCodeUtil.hash(result, getDst());
    result = HashCodeUtil.hash(result, getHttpUri());
    result = HashCodeUtil.hash(result, getHttpMethod());
    result = HashCodeUtil.hash(result, getIsAck());
    result
  }
}

