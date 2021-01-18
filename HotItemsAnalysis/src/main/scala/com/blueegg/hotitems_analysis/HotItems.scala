package com.blueegg.hotitems_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


// 统计最近一小时，按pv统计top 5商品，5分钟统计一次
// 1小时以内的数据做一个排序，取前5条
object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 定义事件时间语义

    // 从文件中读取数据，并转换成样例类，提取时间戳生成watermark
    val inputStream: DataStream[String] = env.readTextFile("/Users/cyd/Desktop/study/flink/retailer-ueba/HotItemsAnalysis/src/main/resources/UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L) // 秒转换成毫秒

    // 得到窗口聚合结果
    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv")  // 过滤pv行为
      .keyBy("itemId") // 按照商品id分组
      .timeWindow(Time.hours(1), Time.minutes(5)) // 设置滑动窗口进行统计
      .aggregate(new CountAgg(), new ItemViewWindowResult())

    val resultStream: DataStream[String] = aggStream
      .keyBy("windowEnd") // 按照窗口分组，收集当前窗口内的商品count数据
      .process(new TopNHotItems(5)) // 自定义处理流程

    resultStream.print()
    env.execute("hot items")
  }
}

