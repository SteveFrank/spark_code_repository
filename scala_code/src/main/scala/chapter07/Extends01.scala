package chapter07

/**
 * @author yangqian
 * @date 2021/5/8
 */
object Extends01 {
  def main(args: Array[String]): Unit = {
    // 使用
    val student = new Student
    student.name = "jack"
    student.studying()
    student.showInfo()
  }
}

class Person {
  // Person
  var name: String = _
  var age: Int = _
  def showInfo(): Unit = {
    println("Person信息如下:")
    println(s"名字:${this.name}")
  }
}

class Student extends Person {
  def studying(): Unit = {
    println(s"${this.name} + 学习scala中")
  }
}
