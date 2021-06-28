package chapter10.list

/**
 * @date 2021/5/8
 */
object ListDemo01 {

  def main(args: Array[String]): Unit = {
    val list01 = List(1, 2, 3)
    println(list01)
    val list02 = Nil
    println(list02)

    println("-------------list 追加元素后的效果-------------")
    val list1 = List(1, 2, 3, "abc")
    val list2 = list1 :+ 4
    println(list2)

  }

}
