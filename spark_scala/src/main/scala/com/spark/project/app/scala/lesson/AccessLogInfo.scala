package com.spark.project.app.scala.lesson

/**
 * @author frankq
 * @date 2021/6/30
 */
class AccessLogInfo(time: Long, up: Long, down: Long) extends Serializable {
  // 时间戳
  var timestamp: Long = time
  // 上行流量
  var upTraffic: Long = up
  // 下行流量
  var downTraffic: Long = down
}
