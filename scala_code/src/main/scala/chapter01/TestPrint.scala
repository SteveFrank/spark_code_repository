package chapter01

/**
 * @date 2021/5/8
 */
object TestPrint {

  def main(args: Array[String]): Unit = {
    var name: String = "tom"
    var sal: Double = 1.2
    println("hello " + sal + name)

    printf("name = %s sal = %f \n", name, sal)
    println(s"第三种方式 name=$name sal=${sal + 1}")
  }

}
