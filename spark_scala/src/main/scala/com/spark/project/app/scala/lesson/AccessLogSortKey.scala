package com.spark.project.app.scala.lesson

/**
 * @author frankq
 * @date 2021/6/30
 */
class AccessLogSortKey(time: Long, up: Long, down: Long) extends Ordered[AccessLogSortKey] with Serializable {

  var timestamp : Long = time
  var upTraffic : Long = up
  var downTraffic : Long = down

  override def compare(that: AccessLogSortKey): Int = {
    if (upTraffic - that.upTraffic != 0) {
      return (upTraffic - that.upTraffic).toInt
    } else if (downTraffic - that.downTraffic != 0) {
      return (downTraffic - that.downTraffic).toInt
    } else if (timestamp - that.timestamp != 0) {
      return (timestamp - that.timestamp).toInt
    }
    0
  }

  override def <(other: AccessLogSortKey): Boolean = {
    if (upTraffic < other.upTraffic) return true
    else if ((upTraffic == other.upTraffic) && downTraffic < other.downTraffic) return true
    else if ((upTraffic == other.upTraffic) && (downTraffic == other.downTraffic) && timestamp < other.timestamp) return true
    return false
  }

  override def >(other: AccessLogSortKey): Boolean = {
    if (upTraffic > other.upTraffic) return true
    else if ((upTraffic == other.upTraffic) && downTraffic > other.downTraffic) return true
    else if ((upTraffic == other.upTraffic) && (downTraffic == other.downTraffic) && timestamp > other.timestamp) return true
    return false
  }

  override def <=(other: AccessLogSortKey): Boolean = {
    if ($less(other)) return true
    else if ((upTraffic == other.upTraffic) && (downTraffic == other.downTraffic) && (timestamp == other.timestamp)) return true
    return false
  }

  override def >=(other: AccessLogSortKey): Boolean = {
    if ($greater(other)) return true
    else if ((upTraffic == other.upTraffic) && (downTraffic == other.downTraffic) && (timestamp == other.timestamp)) return true
    return false
  }

  override def compareTo(that: AccessLogSortKey): Int = {
    if (upTraffic - that.upTraffic != 0) {
      return (upTraffic - that.upTraffic).toInt
    } else if (downTraffic - that.downTraffic != 0) {
      return (downTraffic - that.downTraffic).toInt
    } else if (timestamp - that.timestamp != 0) {
      return (timestamp - that.timestamp).toInt
    }
    0
  }
}
