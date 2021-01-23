package com.blueegg.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

// 为了扩展性，定义list来放前几个成功还是失败的状态
class LoginFailWarningResult(failTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]{
  // 定义状态，保存当前所有的登陆失败事件，保存定时器的时间戳
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-state", classOf[LoginEvent]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    // 判断当前登陆事件是成功还是失败
    if (value.eventType == "fail") {
      loginFailListState.add(value)
      // 如果没有定时器，那么注册一个2秒后的定时器
      if (timerTsState.value() == 0) {
        val ts = value.timestamp * 1000L + 2000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
      }
    }else {
      // 如果是成功，那么直接清空状态和定时器，重新开始
      ctx.timerService().deleteEventTimeTimer(timerTsState.value())
      loginFailListState.clear()
      timerTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {
    val allLoginFailList: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
    val iter = loginFailListState.get().iterator()
    while (iter.hasNext) {
      allLoginFailList += iter.next()
    }
    // 判断登陆失败事件的个数，如果超过了上限，报警
    if (allLoginFailList.length >= failTimes){
      out.collect(
        LoginFailWarning(
          allLoginFailList.head.userId,
          allLoginFailList.head.timestamp,
          allLoginFailList.last.timestamp,
        "login fail in 2s for " + allLoginFailList.length + " times. "))
    }

    // 清空状态
    loginFailListState.clear()
    timerTsState.clear()
  }
}
