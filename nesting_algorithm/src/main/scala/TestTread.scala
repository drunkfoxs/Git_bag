/**
 * Created by liziyao on 15/10/27.
 */
object TestTread {
  def main(args: Array[String]) {

    println("主线程")
    val thre1 = new TreadUse()
    thre1.start()
    Thread.sleep(7000) //7秒

  }


  class TreadUse extends Thread {

    override def run() {
      println("子线程")

      try {
        Thread.sleep(10000)
      }
      catch {
        case e: Exception =>
      }

    }

    class TreadUse extends Thread {

      override def run() {
        println("子线程")

        try {
          Thread.sleep(10000)
        }
        catch {
          case e: Exception =>
        }

      }

    }

  }

}
