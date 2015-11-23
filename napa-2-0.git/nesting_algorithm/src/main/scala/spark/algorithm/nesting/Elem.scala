//package spark.algorithm.nesting
//
///**
// * Created by liziyao on 15/9/5.
// */
//class Elem(arrs:Array[String]) {
//  val DEL = ","
//  var UUID = arrs(0)
//  var client_ip = arrs(1)
//  var client_port = arrs(2)
//  var server_ip = arrs(3)
//  var server_port = arrs(4)
//  var client_in_time = arrs(5)
//  var client_out_time = arrs(6)
//  var server_in_time = arrs(7)
//  var server_out_time = arrs(8)
//
//  var par_uuid = ""
//  var child_uuid = ""
//
//  def setPar(par_uuid:String) = {
//    this.par_uuid = par_uuid
//  }
//
//  def setChil(child_uuid:String) = {
//    this.child_uuid = child_uuid
//  }
//
//  override  def toString() = {
//    return UUID+DEL+client_ip+DEL+client_port+DEL+server_ip+DEL+server_port+DEL+client_in_time+DEL+client_out_time+DEL+server_in_time+DEL+server_out_time
//  }
//
//}
