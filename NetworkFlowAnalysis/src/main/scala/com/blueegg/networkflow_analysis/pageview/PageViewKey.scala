package com.blueegg.networkflow_analysis.pageview

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object PageViewKey {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    // 转换成样例类类型，并提取时间戳和watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L) // 秒转换成毫秒

    val pvStream = dataStream
      .filter(_.behavior == "pv")
      .map(new MyMapper())
      .keyBy(_._1) // 数据平均分配到不同组
      .timeWindow(Time.hours(1)) // 1小时滚动窗口
      .aggregate(new PvCountAgg(), new PvCountWindowResult())

    val totalPvStream = pvStream
        .keyBy(_.windowEnd)
        .sum("count")

    totalPvStream.print()
    env.execute("pv job")
  }
}
