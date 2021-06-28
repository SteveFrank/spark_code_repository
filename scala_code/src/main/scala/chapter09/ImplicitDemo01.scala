package chapter09

import scala.language.implicitConversions

/**
 *
 * 隐式转换函数是以 implicit 关键字声明的带有单个参数的函数。
 * 这种函数将会自动应用，将值从一种类型转换为另一种类型
 * @date 2021/5/8
 */
object ImplicitDemo01 {
  def main(args: Array[String]): Unit = {
    // 利用隐式函数优雅的解决数据类型转换问题

    implicit def f1(d: Double): Int = {
      d.toInt
    }

    val num: Int = 3.5
    println(s"num=${num}")

  }
}
