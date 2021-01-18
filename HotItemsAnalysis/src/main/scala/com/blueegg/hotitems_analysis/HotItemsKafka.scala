package com.blueegg.hotitems_analysis

import java.util.Properties
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


// 统计最近一小时，按pv统计top 5商品，5分钟统计一次
// 1小时以内的数据做一个排序，取前5条
object HotItemsKafka {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 定义事件时间语义

    // 从kafka中读取
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val inputStream = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))

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

    dataStream.print("data")
    aggStream.print("agg")
    resultStream.print()
    env.execute("hot items")
  }
}


