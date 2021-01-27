package com.blueegg.orderpay_detect.tx

import com.blueegg.orderpay_detect.OrderEvent
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object TxMatch {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 读取订单事件数据
    val resource1 = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource1.getPath)
      .map(data => {
        val arr = data.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .keyBy(_.txId)

    // 2. 读取到账事件数据
    val resource2 = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(resource2.getPath)
      .map(data => {
        val arr = data.split(",")
        ReceiptEvent(arr(0), arr(1), arr(2).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .keyBy(_.txId)

    // 3. 合并两条流，进行处理
    val resultStream = orderEventStream.connect(receiptEventStream)
      .process(new TxPayMatchResult())

    resultStream.print("matched")
    resultStream.getSideOutput(new OutputTag[OrderEvent]("unmatched-pay")).print("unmatched pays")
    resultStream.getSideOutput(new OutputTag[ReceiptEvent]("unmatched-receipt")).print("unmatched receipts")

    env.execute("tx match job")
  }
}
