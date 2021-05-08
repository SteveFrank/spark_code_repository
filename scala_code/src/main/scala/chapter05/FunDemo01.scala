package chapter05

/**
 * @author yangqian
 * @date 2021/5/8
 */
object FunDemo01 {
  def main(args: Array[String]): Unit = {
    val n1 = 10
    val n2 = 20
    println(s"res=" + getRes(n1, n2, '-'))
  }
  def getRes(n1: Int, n2: Int, oper: Char) = {
    if (oper == '+') {
      n1 + n2
    } else if (oper == '-') {
      n1 - n2
    } else {
      null
    }
  }
}
