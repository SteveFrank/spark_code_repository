package chapter06

/**
 * @author yangqian
 * @date 2021/5/8
 */
object CatDemo {

  def main(args: Array[String]): Unit = {
    val cat = new Cat
  }

  class Cat {
    var name: String = ""
    // _  表示给 age 一个默认的值 ，如果 Int 默认就是 0
    var age: Int = _
    // _  给 color 默认值，如果 String ,默认是就是""
    var color: String = _
  }

}
