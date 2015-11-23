package NormalTest

import java.io.{InputStreamReader, BufferedReader, PrintWriter}
import java.net.{InetAddress, Socket}
import scala.util.control.Breaks._

/**
 * Created by liziyao on 15/11/9.
 */
object SocketTest {
  def main(args: Array[String]) {
    val server = new Socket("9.186.50.15", 9999)
    val in = new BufferedReader(new InputStreamReader(server.getInputStream))
    val out = new PrintWriter(server.getOutputStream)
    val wt = new BufferedReader(new InputStreamReader(System.in))
    breakable {
      while (true) {

        val str = wt.readLine()
        out.println(str)
        out.flush()
        if (str.equals("end"))
          break()
        println(in.readLine())
      }
    }
  }
}
