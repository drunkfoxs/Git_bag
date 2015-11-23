package spark.topology.objects

import spark.pcap.util.HashCodeUtil

/*
 * "_id": "1"
 * "_type": "edge"
 * "_inV": "10.0.0.2"
 * "_outV": "10.0.0.1"
 * "_label":"Request"
 * "inPort": "8080"
 * "outPort":"42465"
 * "inTime": "time to arrive at _inV"
 * "outTime":"time to leave _outV"
 * "httpUri":" /web?iodine=6150aa78d16e11e4b85b2a2cdfb32acb"
 * "httpMethod": "PUT"
 * 
 */

class SplitFlowEdge extends java.io.Serializable {
  var _id = ""
  var _type = "edge"
  var _inV = ""
  var _outV = ""
  var _label = ""
  var inPort = -1
  var outPort = -1
  var inTime = -1l
  var outTime = -1l
  var httpUri = ""
  var httpMethod = ""
  var protocol = ""
  var isRequest = false
  var isResponse = true
  var responseTme = 0l
  
  def setResponseTime(responseTime: Long) {
    this.responseTme = responseTime
  }
  
  def getResponseTime(): Long = {
    this.responseTme
  }
    
  def getId(): String = {
    return this._id
  }
  def setId(n: String) {
    this._id = n
  }  
  def getProtocol(): String = {
    return this.protocol
  }
  def setProtocol(n: String) {
    this.protocol = n
  }    
  def getType(): String = {
    return this._type
  }
  /*
   * setType not supported
   */
//  def setType(n: String) {
//    this._type = n
//  }
  def getInV(): String = {
    return this._inV
  }
  def setInV(n: String) {
    this._inV = n
  }
  def getOutV(): String = {
    return this._outV
  }
  def setOutV(n: String) {
    this._outV = n
  }  
  def getLabel(): String = {
    return this._label
  }
  def setLabel(n: String) {
    this._label = n
  }   
  def getInPort(): Integer = {
    return this.inPort
  }
  def setInPort(port: Integer) {
    this.inPort = port
  }    
   def getOutPort(): Integer = {
    return this.outPort
  }
  def setOutPort(port: Integer) {
    this.outPort = port
  }     
  def setInTime(time: Long){
    this.inTime = time
  }
  def getInTime(): Long = {
    return this.inTime
  }  
  def setOutTime(time: Long){
    this.outTime = time
  }
  def getOutTime(): Long = {
    return this.outTime
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
  def getIsRequest(): Boolean = {
    return this.isRequest
  }
  def setIsRequest(n: Boolean) {
    this.isRequest = n
  }  
  def getIsResponse(): Boolean = {
    return this.isResponse
  }
  def setIsResponse(n: Boolean) {
    this.isResponse = n
  }  
  
  override def toString(): String = {
    return ("(_id = " + this._id + ", _type = " + this._type + ", _inV = " + this._inV 
        + ", _outV = " + this._outV + ", _label = " + this._label + ", inPort = " 
        + this.inPort + ", outPort = " + this.outPort + ", inTime = " + this.inTime 
        + ", outTime = " + this.outTime + ", httpUri = " + this.httpUri
        + ", httpMethod = " + this.httpMethod + ", isRequest = " + this.isRequest
        + ", isResponse = " + this.isResponse + ", protocol= " + protocol +")") 
  }
    def canEqual(other: Any): Boolean =
    other.isInstanceOf[SplitFlowEdge]

  override def equals(other: Any): Boolean =
    other match {
      case that: SplitFlowEdge =>
        (that canEqual this) &&
          this.getHttpMethod() == that.getHttpMethod() &&
          this.getHttpUri() == that.getHttpUri() &&
          this.getId() == that.getId() &&
          this.getInPort() == that.getInPort() &&
          this.getInV() == that.getInV() &&
          this.getIsRequest() == that.getIsRequest() &&
          this.getIsResponse() == that.getIsResponse() &&
          this.getOutPort() == that.getOutPort() &&
          this.getOutTime() == that.getOutTime() &&
          this.getOutV() == that.getOutV() &&
          this.getProtocol() == that.getProtocol()
      case _ => false
    }

  override def hashCode: Int = {
    var result = HashCodeUtil.SEED;
    
    result = HashCodeUtil.hash(result, getHttpMethod());
    result = HashCodeUtil.hash(result, getHttpUri());
    result = HashCodeUtil.hash(result, getId);
    result = HashCodeUtil.hash(result, getInPort());
    result = HashCodeUtil.hash(result, getInV());
    result = HashCodeUtil.hash(result, getIsRequest());
    result = HashCodeUtil.hash(result, getIsResponse());
    result = HashCodeUtil.hash(result, getOutPort());
    result = HashCodeUtil.hash(result, getOutTime());
    result = HashCodeUtil.hash(result, getOutV());
    result = HashCodeUtil.hash(result, getProtocol());
    result
  }
}