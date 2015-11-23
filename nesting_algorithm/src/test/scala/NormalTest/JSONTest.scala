package NormalTest

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ArrayBuffer


/**
 * Created by liziyao on 15/11/5.
 */
object JSONTest {
  def main(args: Array[String]) {

    val jsonStr = new HTTPTest().getResult()
    val json = parse(jsonStr)
    val count = render(json \ "hits" \ "hits" \ "_source" \ "log").children
    var arrBuff = new ArrayBuffer[String]()
    count.foreach(x => {
      val log = x.values.toString
      arrBuff += log.replaceAll("\000", "").replaceAll("\n", "")
    })
//    arrBuff.foreach(x => println(x))

  }
}