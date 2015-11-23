package spark.topology.objects

import spark.pcap.util.HashCodeUtil

class SplitFlowChildID extends java.io.Serializable{
  /*
   * "name": "the ip of this parent node"
   * "childInTime": "the time when the request arrival at this node"
   * "childOutTime": "the time when the request leave this node"
   * 
   */  
  var name = ""
  var childInTime = -1l
  var childOutTime = -1l
    
  def getName(): String = {
    return this.name
  }
  def setName(n: String) {
    this.name = n
  }    
  def getChildInTime(): Long = {
    return this.childInTime
  }
  def setChildInTime(timestamp: Long) {
    this.childInTime = timestamp
  }  
   def getChildOutTime(): Long = {
    return this.childOutTime
  }
  def setChildOutTime(timestamp: Long) {
    this.childOutTime = timestamp
  }   
  override def toString(): String = {
    return ("name = " + this.name + ", childInTime = " + this.childInTime + ", childOutTime = " + this.childOutTime) 
  }    
    def canEqual(other: Any): Boolean =
    other.isInstanceOf[SplitFlowChildID]

  override def equals(other: Any): Boolean =
    other match {
      case that: SplitFlowChildID =>
        (that canEqual this) &&
          this.getChildInTime() == that.getChildInTime() &&
          this.getChildOutTime() == that.getChildOutTime() &&
          this.getName() == that.getName()
      case _ => false
    }

  override def hashCode: Int = {
    var result = HashCodeUtil.SEED;
    
    result = HashCodeUtil.hash(result, getChildInTime());
    result = HashCodeUtil.hash(result, getChildOutTime());
    result = HashCodeUtil.hash(result, getName);
    result
  }
}