package chapter02

/**
 * @author yangqian
 * @date 2021/5/8
 */
object VarDemo02 {

  def main(args: Array[String]): Unit = {
    // 类型推导
    // 这时num就是Int
    var num = 10
    // 类型确定后便不可以再修改，说明scala是强语言
    println(num.isInstanceOf[Int])
    // 在申明和定义一个变量的时候，可以使用var或者val来修饰
    // var修饰变量可变，val修饰变量不可变

    // 值可变 var
    var age = 10
    age = 30

    val num2 = 30
    // 值不可变
//    num2 = 40

    // 为什么在实际的设计中 var和val 两者都有呢
    // val 更加常用，在实际的编程中，更多的需求在 创建/获取 一个对象之后，读取该对象的属性
    // 很少修改对象本身，都是调整其中的值
    // val 是没有线程安全问题的

    val dog = new Dog
    dog.age = 9
    dog.name = "旺财"

    // val 等同于加上了final
  }

}

class Dog {
  var age: Int = 0
  var name: String = ""
}