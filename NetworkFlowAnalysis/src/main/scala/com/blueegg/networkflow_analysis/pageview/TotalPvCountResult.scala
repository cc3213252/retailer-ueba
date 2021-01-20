package com.blueegg.networkflow_analysis.pageview

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCount, PvCount]{
  // 定义一个状态，保存当前所有count总和
  lazy val totalPvCountResultState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-pv", classOf[Long]))

  override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]): Unit = {
    // 每来一个数据， 将count值叠加在当前的状态上
    val currentTotalCount = totalPvCountResultState.value()
    totalPvCountResultState.update(currentTotalCount + value.count)
    // 注册一个windowEnd + 1ms后触发的定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
    val totalPvCount = totalPvCountResultState.value()
    out.collect(PvCount(ctx.getCurrentKey, totalPvCount))
    totalPvCountResultState.clear()
  }
}
