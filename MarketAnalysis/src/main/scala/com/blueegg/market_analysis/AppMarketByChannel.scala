package com.blueegg.market_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object AppMarketByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource(new SimulatedSource)
      .assignAscendingTimestamps(_.timestamp)

    // 开窗统计输出
    val resultStream = dataStream
      .filter(_.behavior != "uninstall")
      .keyBy(data => (data.channel, data.behavior)) // 需要按两个字段分组的场景
      .timeWindow(Time.days(1), Time.seconds(5))
      .process(new MarketCountByChannel())

    resultStream.print()
    env.execute("app market by channel job")
  }
}
