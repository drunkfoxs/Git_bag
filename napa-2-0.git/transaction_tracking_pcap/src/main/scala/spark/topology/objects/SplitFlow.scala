package spark.topology.objects

import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import spark.pcap.util.HashCodeUtil

class SplitFlow extends java.io.Serializable {
  var splitflowAttributes = new SplitFlowAttributes()
  var splitFlowParentID = new SplitFlowParentID()
  var splitFlowChildID = new SplitFlowChildID()
  val splitFlowEdges: Map[String, SplitFlowEdge] = new HashMap[String, SplitFlowEdge]()
  val splitFlowNodes: Map[String, SplitFlowNode] = new HashMap[String, SplitFlowNode]()
  //  val splitFlowParentID:Map[String, SplitFlowParentID] = new HashMap[String, SplitFlowParentID]() 
  //  val splitFlowChildID:Map[String, SplitFlowChildID] = new HashMap[String, SplitFlowChildID]() 

  def getSplitFlowAttributes(): SplitFlowAttributes = {
    return this.splitflowAttributes;
  }

  def setSplitFlowAttributes(properties: SplitFlowAttributes) {
    this.splitflowAttributes = properties;
  }
  def getSplitFlowEdge(): Map[String, SplitFlowEdge] = {
    return this.splitFlowEdges;
  }
  def updateSplitFlowEdge(index: String, edge: SplitFlowEdge) {
    splitFlowEdges.put(index, edge)
  }
  def getSplitFlowNodes(): Map[String, SplitFlowNode] = {
    return this.splitFlowNodes
  }
  def updateSplitFlowNode(index: String, node: SplitFlowNode) {
    splitFlowNodes.put(index, node)
  }
  def getSplitFlowParentID(): SplitFlowParentID = {
    return this.splitFlowParentID
  }
  def setSplitFlowParentID(properties: SplitFlowParentID) {
    this.splitFlowParentID = properties
  }
  def getSplitFlowChildID(): SplitFlowChildID = {
    return this.splitFlowChildID
  }
  def setSplitFlowChildID(properties: SplitFlowChildID) {
    this.splitFlowChildID = properties
  }

  override def toString(): String = {
    return ("( " + this.getSplitFlowAttributes + " )")
  }

  def canEqual(other: Any): Boolean =
    other.isInstanceOf[SplitFlow]

  override def equals(other: Any): Boolean =
    other match {
      case that: SplitFlow =>
        (that canEqual this) &&
          this.getSplitFlowAttributes() == that.getSplitFlowAttributes() &&
          this.getSplitFlowChildID() == that.getSplitFlowChildID() &&
          this.getSplitFlowParentID() == that.getSplitFlowParentID() &&
          this.getSplitFlowEdge() == that.getSplitFlowEdge() &&
          this.getSplitFlowNodes() == that.getSplitFlowNodes()
      case _ => false
    }

  override def hashCode: Int = {
    var result = HashCodeUtil.SEED;

    result = HashCodeUtil.hash(result, getSplitFlowAttributes());
    result = HashCodeUtil.hash(result, getSplitFlowChildID());
    result = HashCodeUtil.hash(result, getSplitFlowParentID);
    result = HashCodeUtil.hash(result, getSplitFlowEdge());
    result = HashCodeUtil.hash(result, getSplitFlowNodes());
    result
  }
}
