package chapter04

/**
 * @date 2021/5/8
 */
object ForUtilDemo02 {

  def main(args: Array[String]): Unit = {
    val start = 1
    val end = 11
    // 循环范围 start -- (end - 1)
    for (i <- start until end) {
      println(s"hello $i")
    }
  }

}
