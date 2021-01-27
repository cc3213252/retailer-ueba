package com.blueegg.orderpay_detect.tx

import com.blueegg.orderpay_detect.OrderEvent
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.util.Collector

class TxMatchWithJoinResult() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left, right))
  }
}
