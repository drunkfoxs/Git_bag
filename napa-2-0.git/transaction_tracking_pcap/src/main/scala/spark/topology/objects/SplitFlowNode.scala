package spark.topology.objects

import spark.pcap.util.HashCodeUtil

class SplitFlowNode extends java.io.Serializable {
  /*
   * "_id": "10.0.0.1"
   * "_type": "vertex"
   * "ip": "10.0.0.1"
   * "name":"client"
   * 
   */
  var _id = ""
  var _type = "vertex"
  var ip = ""
  var name = ""

  def getId(): String = {
    return this._id
  }
  def setId(n: String) {
    this._id = n
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
  def getIp(): String = {
    return this.ip
  }
  def setIp(n: String) {
    this.ip = n
  }
  def getName(): String = {
    return this.name
  }
  def setName(n: String) {
    this.name = n
  }
  
  override def toString(): String = {
    return ("(_id = " + this._id + ", _type = " + this._type + ", ip = " + this.ip
      + ", name = " + this.name + ")")
  }

  def canEqual(other: Any): Boolean =
    other.isInstanceOf[SplitFlowNode]

  override def equals(other: Any): Boolean =
    other match {
      case that: SplitFlowNode =>
        (that canEqual this) &&
          _id == that._id &&
          _type == that._type &&
          name == that.name &&
          ip == that.ip
        case _ => false
    }

   override def hashCode: Int = {
         var result = HashCodeUtil.SEED;
    
    result = HashCodeUtil.hash(result, _id);
    result = HashCodeUtil.hash(result, _type);
    result = HashCodeUtil.hash(result, name);
    result = HashCodeUtil.hash(result, ip);
    result

   }
}