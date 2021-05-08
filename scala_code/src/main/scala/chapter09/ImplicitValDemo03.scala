package chapter09

/**
 *
 * 编译器的优先级为 传值 >  隐式值 >  默认值
 *
 * @author yangqian
 * @date 2021/5/8
 */
object ImplicitValDemo03 {
  def main(args: Array[String]): Unit = {
    implicit val str1: String = "jack ~ " //这个就是隐式值

    //implicit name: String ：name 就是隐式参数
    def hello(implicit name: String): Unit = {
      println(name + " hello")
    }

    hello //底层 hello$1(str1);
  }
}
