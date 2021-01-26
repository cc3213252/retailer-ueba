package com.blueegg.orderpay_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object OrderTimeoutWithoutCep {
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

    // 自定义ProcessFunction进行复杂事件的检测
    val orderResultStream = orderEventStream
      .keyBy(_.orderId)
      .process(new OrderPayMatchResult())

    orderResultStream.print("payed")
    orderResultStream.getSideOutput(new OutputTag[OrderResult]("timeout")).print("timeout")
    env.execute("order timeout without cep")
  }
}
