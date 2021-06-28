package chapter08

/**
 *
 * 说明
 * 1. 当在同一个文件中，有 class ScalaPerson 和 object ScalaPerson
 * 2. class ScalaPerson 称为伴生类,将非静态的内容写到该类中
 * 3. object ScalaPerson 称为伴生对象,将静态的内容写入到该对象(类)
 * 4. class ScalaPerson 编译后底层生成 ScalaPerson 类 ScalaPerson.class
 * 5. object ScalaPerson 编译后底层生成 ScalaPerson$类 ScalaPerson$.class
 * 6. 对于伴生对象的内容，我们可以直接通过 ScalaPerson.属性 或者方法
 *
 * @date 2021/5/8
 */
object AccompanyObject {

  def main(args: Array[String]): Unit = {
    println(ScalaPerson.sex)
    ScalaPerson.sayHi()
  }

}

class ScalaPerson {
  var name: String = _
}

object ScalaPerson {
  var sex: Boolean = true
  def sayHi(): Unit = {
    println("object ScalaPerson sayHi ~~")
  }
}