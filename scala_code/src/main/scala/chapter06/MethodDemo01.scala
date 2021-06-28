package chapter06

/**
 * @date 2021/5/8
 */
object MethodDemo01 {
  def main(args: Array[String]): Unit = {
    val dog = new Dog
    println(dog.cal(10, 20))
  }
}

class Dog {
  private var sal: Double = _
  var food: String = _

  def cal(n1: Int, n2: Int): Int = {
    return n1 + n2
  }
}
