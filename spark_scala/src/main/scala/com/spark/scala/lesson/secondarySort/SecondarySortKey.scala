package com.spark.scala.lesson.secondarySort

/**
 * @author yangqian
 * @date 2021/5/9
 */
class SecondarySortKey(val first: Int, val second: Int) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(that: SecondarySortKey): Int = {
    if (this.first - that.first!= 0) {
      this.first - that.first
    } else {
      this.second - that.second
    }
  }
}
