package NormalTest

/**
 * Created by liziyao on 15/11/10.
 */
object TressTest {
  def main(args: Array[String]) {

    val data = List((58, 63), (59, 63), (60, 63), (63, 64), (61, 64), (62, 64))


    val ss = "\\\\u0000\\\\u0000\\\\u0000"

    val aa = ss.replaceAll("\\\\u0000","a").replaceAll("\\\\","")

    println(aa)

  }
}
