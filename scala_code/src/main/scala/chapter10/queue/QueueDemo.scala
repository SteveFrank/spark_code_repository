package chapter10.queue

import scala.collection.mutable

/**
 * @date 2021/5/8
 */
object QueueDemo {

  def main(args: Array[String]): Unit = {
    val q1 = new mutable.Queue[Int]
    println(q1)

    q1 += 9
    println(s"q1=$q1")
    q1 ++= List(4, 5, 7)
    println("q1=" + q1)

    val queueElement = q1.dequeue()
    println(s"queueElement=$queueElement,q1=$q1")

    println("============Queue-返回队列的元素=================")
    //队列 Queue-返回队列的元素
    //1. 获取队列的第一个元素
    println(q1.head) // 4, 对 q1 没有任何影响
    //2. 获取队列的最后一个元素
    println(q1.last) // 888, 对 q1 没有任何影响
    //3. 取出队尾的数据 ,即：返回除了第一个以外剩余的元素，可以级联使用
    println(q1.tail)
    println(q1.tail.tail)

  }

}
