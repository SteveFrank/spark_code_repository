package chapter10.list

import scala.collection.mutable.ListBuffer

/**
 * @date 2021/5/8
 */
object ListBufferDemo01 {

  def main(args: Array[String]): Unit = {
    val list0 = ListBuffer[Int](1,2,3)
    println("list0(2)=" + list0(2))
    for (item <- list0) {
      println(s"item=$item")
    }
  }

}
