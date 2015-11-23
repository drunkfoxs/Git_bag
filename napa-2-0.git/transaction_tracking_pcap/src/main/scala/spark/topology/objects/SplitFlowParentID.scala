package spark.topology.objects

import spark.pcap.util.HashCodeUtil

class SplitFlowParentID extends java.io.Serializable{
  /*
   * "name": "the ip of this parent node"
   * "parentInTime": "the time when the request arrival at this node"
   * "parentOutTime": "the time when the request leave this node"
   * 
   */  
  var name = ""
  var parentInTime = -1l
  var parentOutTime = -1l
    
  def getName(): String = {
    return this.name
  }
  def setName(n: String) {
    this.name = n
  }    
  def getParentInTime(): Long = {
    return this.parentInTime
  }
  def setParentInTime(timestamp: Long) {
    this.parentInTime = timestamp
  }  
   def getParentOutTime(): Long = {
    return this.parentOutTime
  }
  def setParentOutTime(timestamp: Long) {
    this.parentOutTime = timestamp
  }   
  
  override def toString(): String = {
    return ("name = " + this.name + ", parentInTime = " + this.parentInTime + ", parentOutTime = " + this.parentOutTime) 
  }    
  
    def canEqual(other: Any): Boolean =
    other.isInstanceOf[SplitFlowParentID]

  override def equals(other: Any): Boolean =
    other match {
      case that: SplitFlowParentID =>
        (that canEqual this) &&
          this.getName() == that.getName() &&
          this.getParentInTime() == that.getParentInTime() &&
          this.getParentOutTime() == that.getParentOutTime()
      case _ => false
    }

  override def hashCode: Int = {
    var result = HashCodeUtil.SEED;
    
    result = HashCodeUtil.hash(result, getName());
    result = HashCodeUtil.hash(result, getParentInTime());
    result = HashCodeUtil.hash(result, getParentOutTime);
    result
  }
  
}