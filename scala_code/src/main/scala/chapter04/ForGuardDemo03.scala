package chapter04

/**
 * @date 2021/5/8
 */
object ForGuardDemo03 {

  def main(args: Array[String]): Unit = {
    // 循环守卫，循环保护模式，保护模式为true的进入循环体内部
    // false的则跳过，类似于continue
    for (i <- 1 to 3 if i != 2) {
      println(s"$i ")
    }
  }

}
