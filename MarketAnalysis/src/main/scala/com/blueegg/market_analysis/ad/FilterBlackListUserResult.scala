package com.blueegg.market_analysis.ad

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

// 自定义KeyedProcessFunction
class FilterBlackListUserResult(maxCount: Long) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]{
  // 定义状态，保存用户对广告的点击量，每天0点定时清空状态的时间戳，标记当前用户是否已经进入黑名单
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  lazy val resetTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-ts", classOf[Long]))
  lazy val isBlackState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-black", classOf[Boolean]))

  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    val curCount = countState.value()

    // 判断只要是第一个数据来了，直接注册0点的清空状态定时器
    if (curCount == 0) {
      // ctx.timerService().currentProcessingTime()/(1000 * 60 * 60 * 24) //1970至今的一个整天数，再加1表示明天，减8表示东8区
      val ts = (ctx.timerService().currentProcessingTime()/(1000 * 60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000// 明天第一个时间点开始的时间戳
      resetTimerTsState.update(ts)
      ctx.timerService().registerProcessingTimeTimer(ts)
    }

    // 判断count值是否已经达到定义的阈值，如果超过就输出到黑名单
    if (curCount >= maxCount) {
      // 判断是否已经在黑名单里，没有的话才输出侧输出流
      if (!isBlackState.value()){
        isBlackState.update(true)
        ctx.output((new OutputTag[BlackListUserWarning]("warning")), BlackListUserWarning(value.userId, value.adId, "Click ad over" + maxCount + "times today."))
      }
      return
    }

    // 正常情况，count加1，然后将数据原样输出
    countState.update(curCount + 1)
    out.collect(value)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    if (timestamp == resetTimerTsState.value()) {
      isBlackState.clear()
      countState.clear()
    }
  }
}
