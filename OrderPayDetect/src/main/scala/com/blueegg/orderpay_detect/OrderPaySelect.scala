package com.blueegg.orderpay_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction

class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult]{
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = pattern.get("pay").iterator().next().orderId
    OrderResult(payedOrderId, "payed successfully")
  }
}
