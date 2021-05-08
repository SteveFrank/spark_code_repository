package chapter08

import chapter08.Child02.joinGame

/**
 * @author yangqian
 * @date 2021/5/8
 */
object ChildJoinGame {
  def main(args: Array[String]): Unit = {
    val child0 = new Child02("1")
    val child1 = new Child02("2")
    val child2 = new Child02("3")

    Child02.joinGame(child0)
    Child02.joinGame(child1)
    Child02.joinGame(child2)

    Child02.showNum()

  }
}
