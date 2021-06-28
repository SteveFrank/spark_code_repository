package chapter04

/**
 * @date 2021/5/8
 */
object ForDemo01 {

  def main(args: Array[String]): Unit = {
    val start = 1
    val end = 10
    // 1. start  从哪个数开始循环
    // 2. to 是关键字
    // 3. end 循环结束的值
    // 4. start to end 表示前后闭合
    for (i <- start to end) {
      println(i)
    }
  }

}
