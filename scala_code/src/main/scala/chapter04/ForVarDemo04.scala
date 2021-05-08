package chapter04

/**
 * @author yangqian
 * @date 2021/5/8
 */
object ForVarDemo04 {

  def main(args: Array[String]): Unit = {
    for (i <- 1 to 3; j = 4 - i) {
      println(s"j = $j and i = $i")
    }
  }

}
