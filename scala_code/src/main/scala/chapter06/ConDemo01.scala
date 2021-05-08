package chapter06

/**
 * @author yangqian
 * @date 2021/5/8
 */
object ConDemo01 {
  def main(args: Array[String]): Unit = {
    val person = new Person(inName = "tom", inAge = 10)
    println(person)
  }
}

class Person(inName: String, inAge: Int) {
  var name: String = inName
  var age: Int = inAge
  age += 10
  println("~~~~~~~~~~~~~~")

  override def toString: String = {
    s"name=${this.name} \t age=${this.age}"
  }
}
