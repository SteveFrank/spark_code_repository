package chapter10.map

import scala.collection.mutable

/**
 * @date 2021/5/8
 */
object MapDemo01 {

  def main(args: Array[String]): Unit = {
    val map1 = Map("Alice" -> 10, "Bob" -> 20, "Kotlin" -> "北京")
    println(map1)

    val map2 = mutable.Map("Alice" -> 10, "Bob" -> 20, "Kotlin" -> "北京")
    println(map2)

    val map3 = new scala.collection.mutable.HashMap[String, Int]
    println(map3)

    val map4 = mutable.Map(("Alice" , 10), ("Bob" , 20), ("Kotlin" , "北京"))
    println("map4=" + map4)

    val value1 = map2("Alice")
    println(value1)


  }

}
