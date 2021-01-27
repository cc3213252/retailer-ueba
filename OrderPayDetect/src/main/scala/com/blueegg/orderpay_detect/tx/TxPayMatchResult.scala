package com.blueegg.orderpay_detect.tx

import com.blueegg.orderpay_detect.OrderEvent
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class TxPayMatchResult() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  // 定义状态，保存当前交易对应的订单支付事件和到账事件
  lazy val payEventState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))
  lazy val receiptEventState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))
  // 侧输出流标签
  val unmatchedPayEventOutputTag = new OutputTag[OrderEvent]("unmatched-pay")
  val unmatchedReceiptEventOutputTag = new OutputTag[ReceiptEvent]("unmatched-receipt")

  override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 订单支付来了，要判断之前是否有到账事件
    val receipt = receiptEventState.value()
    if (receipt != null){
      // 如果已经有receipt，正常输出匹配，清空状态
      out.collect((pay, receipt))
      receiptEventState.clear()
      payEventState.clear()
    }else{
      // 如果还没来，注册定时器开始等待5秒
      ctx.timerService().registerEventTimeTimer(pay.timestamp * 1000L + 5000L)
      // 更新状态
      payEventState.update(pay)
    }
  }

  override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 到账事件来了，要判断之前是否有pay事件
    val pay = payEventState.value()
    if (pay != null){
      // 如果已经有receipt，正常输出匹配，清空状态
      out.collect((pay, receipt))
      receiptEventState.clear()
      payEventState.clear()
    }else{
      // 如果还没来，注册定时器开始等待3秒
      ctx.timerService().registerEventTimeTimer(receipt.timestamp * 1000L + 3000L)
      // 更新状态
      receiptEventState.update(receipt)
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 定时器触发，判断状态中哪个还存在，就代表另一个没来，输出到侧输出流
    if (payEventState.value() != null){
      ctx.output(unmatchedPayEventOutputTag, payEventState.value())
    }
    if (receiptEventState.value() != null){
      ctx.output(unmatchedReceiptEventOutputTag, receiptEventState.value())
    }
    // 清空状态
    receiptEventState.clear()
    payEventState.clear()
  }
}
