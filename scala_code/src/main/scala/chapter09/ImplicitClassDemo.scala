package chapter09

/**
 * @author yangqian
 * @date 2021/5/8
 */
object ImplicitClassDemo {
  def main(args: Array[String]): Unit = {
    implicit class DB1(val m: MySQL1) {
      def addSuffix(): String = {
        m + "scala"
      }
    }

    val mySQL1 = new MySQL1
    mySQL1.sayOk()
    mySQL1.addSuffix()

  }
}

class DB1 {}

class MySQL1 {
  def sayOk(): Unit = {
    println("MySQL1.sayOk")
  }
}
