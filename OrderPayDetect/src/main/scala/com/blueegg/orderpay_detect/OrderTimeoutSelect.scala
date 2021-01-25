package com.blueegg.orderpay_detect

import java.util

import org.apache.flink.cep.PatternTimeoutFunction

// 实现自定义的PatternTimeoutFunction以及PatternSelectFunction
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult]{
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
    val timeoutOrderId = pattern.get("create").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout " + ": +" + timeoutTimestamp)
  }
}
