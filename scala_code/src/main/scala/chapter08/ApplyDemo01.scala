package chapter08

/**
 * @author yangqian
 * @date 2021/5/8
 */
object ApplyDemo01 {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3)
    println(list)

    val pig = new Pig("小花")
    println("pig.name=" + pig.name)

    //使用 apply 方法来创建对象
    val pig2 = Pig("小黑猪") //自动	apply(pName: String)
    val pig3 = Pig() // 自动触发 apply()

    println("pig2.name=" + pig2.name) //小黑猪
    println("pig3.name=" + pig3.name) //匿名猪猪

  }
}
