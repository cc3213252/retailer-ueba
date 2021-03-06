package com.blueegg.networkflow_analysis

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

// 统计10分钟内，访问前三的url，5秒统计一次
// 数据时间格式转换，过滤复杂数据
object HotPagesNetworkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("/Users/cyd/Desktop/study/flink/retailer-ueba/NetworkFlowAnalysis/src/main/resources/apache.log")
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val ts = simpleDateFormat.parse(arr(3)).getTime  // 已经是毫秒
        ApacheLogEvent(arr(0), arr(1), ts, arr(5), arr(6))
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.minutes(1)) {
      override def extractTimestamp(element: ApacheLogEvent): Long = element.timestamp
    })

    // 进行开窗聚合，以及排序输出
    val aggStream = dataStream
      .filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new PageCountAgg(), new PageViewCountWindowResult())

    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotPages(3))

    resultStream.print()
    env.execute("hot pages job")
  }
}
