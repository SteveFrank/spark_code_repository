package chapter05

/**
 * @date 2021/5/8
 */
object LazyDemo01 {

  def main(args: Array[String]): Unit = {
    lazy val res = sum(10, 20)
    println(res)
  }

  def sum(n1: Int, n2: Int): Int = {
    println("sum() 执行了... ...")
    return n1 + n2
  }

}
