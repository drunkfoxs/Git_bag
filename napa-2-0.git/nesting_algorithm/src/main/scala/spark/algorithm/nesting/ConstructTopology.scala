//package spark.algorithm.nesting
//
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
//
//import scala.collection.mutable.ArrayBuffer
//
//object ConstructTopology {
//  def main(args: Array[String]) {
//    val master = "local[2]"
////    val file = "data/sample.json"
//    val conf = new SparkConf().setMaster(master).setAppName("Simple Application")
//    val sc = new SparkContext(conf)
////    val sample = sc.textFile(file, 2).cache()
//
//    val data = sc.textFile("/Git_bag/napa-2-0.git/nesting_algorithm/data/change").map(_.split(",")).cache()
//
//    val data_object = data.map( x => {
//      val Elems = new Elem(x)
//      Elems
//    })
//
//    val dataList = data_object.toArray()
//
//    val result = data_object.map( x => {
//      var elemBuff = ArrayBuffer[(Elem,Elem)]()
//      for ( i <- 0 to dataList.size-1){
//        if( x.server_ip == dataList(i).client_ip )
//          elemBuff = elemBuff :+ (x,dataList(i))
//      }
//      elemBuff
//    }).filter( x => x.size>0 )
//
//    //result: [(001,002),(001,003),(001,004)],[(002,005)]
//
//    val result_2 = result.map( x => x.mkString(",")).collect().mkString(",").split(",")
//
//    val check = result_2
//
//    def getRoute(elms:(Elem,Elem),arrs:Array[(Elem,Elem)]) = {
//      var elemBuff = ArrayBuffer[(Elem,Elem)]()
//      elemBuff = elemBuff :+ elms
//      for( i <- 0 to arrs.size-1) {
//        if ( elms._2.server_ip == arrs(i)._2.client_ip)
//          elemBuff = elemBuff :+ arrs(i)
//      }
//      elemBuff
//    }
//
//    val result_3 = result_2.map( x => getRoute(x,check))
//
//    //TODO: add construction logic
//    /*
//     * Expect output:[(001,002),(002,005)]
//     */
//  }
//}
