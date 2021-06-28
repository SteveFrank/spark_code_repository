package chapter07

/**
 * @date 2021/5/8
 */
object TypeConvert {

  def main(args: Array[String]): Unit = {
    println(classOf[String])
    val s = "king"
    println(s.getClass.getName)
    var p1 = new Person200
    var emp = new Emp200

    p1.printName()
    p1.sayHi()
    // 将子类引用交给了父类（向上转型，自动）
    p1 = emp
    p1.printName()
    p1.sayHi()

    // 将父类的引用重新转成子类引用(多态),即向下转型
    var emp2 = p1.asInstanceOf[Emp200]
    emp2.sayHi()

  }

}

class Person200 {
  var name: String = "tom"
  def printName(): Unit = {
    println(s"Person printName() ${this.name} ")
  }
  def sayHi(): Unit = {
    println("Person200 sayHi ~")
  }
}

class Emp200 extends Person200 {
  override def printName(): Unit = {
    println(s"Emp200 printName() ${this.name}")
  }

  override def sayHi(): Unit = {
    println("Emp200 sayHi ~~~")
  }
}


