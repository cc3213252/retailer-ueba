package com.blueegg.networkflow_analysis.uv

import com.blueegg.networkflow_analysis.pageview.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/UserBehavior.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    // 转换成样例类类型，并提取时间戳和watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L) // 秒转换成毫秒

    val uvStream = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1)) // 直接不分组，基于dataStream开1小时滚动窗口
      .apply(new UvCountResult())

    uvStream.print()
    env.execute("uv job")
  }
}
