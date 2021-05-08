package chapter04

/**
 * @author yangqian
 * @date 2021/5/8
 */
object ForExercise01 {
  def main(args: Array[String]): Unit = {
    val start = 1
    val end = 100
    var sum = 0
    var count = 0
    for (i <- start to end) {
      sum += i
      if (i % 9 == 0) {
        count += 1
      }
    }
    println(s"sum:$sum, count:$count")
  }
}
