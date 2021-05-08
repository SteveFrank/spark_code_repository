package chapter04

/**
 * @author yangqian
 * @date 2021/5/8
 */
object YieldForDemo05 {

  def main(args: Array[String]): Unit = {
    // 在遍历的过程中处理的结果返回到一个新的Vector集合中，使用yield关键字
    //说明 val res = for(i <- 1 to 10) yield i 含义
    //1. 对 1 to 10 进行遍历
    //2. yield i  将每次循环得到 i  放入到集合 Vector 中，并返回给 res
    //3. i 这里是一个代码块，这就意味我们可以对 i 进行处理
    //4. 下面的这个方式，就体现出 scala 一个重要的语法特点，就是将一个集合中个各个数据
    val res = for (i <- 1 to 10) yield {
      if (i % 2 == 0) {
        i
      } else {
        "非偶"
      }
    }
    // for 推导式有一个不成文的约定：当 for 推导式仅包含单一表达式时使用圆括号，当其包含多个表达式时使用大括号
    // 当使用{} 来换行写表达式时，分号就不用写了
    println(res)
  }

}
