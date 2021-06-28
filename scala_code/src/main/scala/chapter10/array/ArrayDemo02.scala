package chapter10.array

/**
 * @date 2021/5/8
 */
object ArrayDemo02 {
  def main(args: Array[String]): Unit = {
    var arr02 = Array(1, 3, "xx")
    arr02(1) = "xx"
    for (i <- arr02) {
      println(i)
    }

    for (index <- arr02.indices) {
      println(s"arr02[${index}]=${arr02(index)} \t")
    }
  }
}
