package com.blueegg.market_analysis.ad

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object AdClickAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 从文件中读取数据
    val resource = getClass.getResource("/AdClickLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

    // 转换成样例类，并提取时间戳和watermark
    val adLogStream = inputStream
      .map(data => {
        val arr = data.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 开窗聚合统计
    val adCountResultStream = adLogStream
      .keyBy(_.province)
      .timeWindow(Time.days(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountWindowResult())

    adCountResultStream.print()
    env.execute("ad count statistics job")
  }
}
