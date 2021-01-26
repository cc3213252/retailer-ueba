package com.blueegg.orderpay_detect

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

// watermark是1毫秒
object OrderTimeOut {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 0. 从文件中读取数据
    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val arr = data.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .keyBy(_.orderId)

    // 1. 定义一个pattern
    val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 2. 将pattern应用到数据流上，进行模式检测
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    // 3. 定义侧输出流标签，用于处理超时事件
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

    // 4. 调用select方法，提取并处理匹配的成功支付事件以及超时事件
    val resultStream = patternStream.select(orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect())

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")
    env.execute("order timeout job")
  }
}
