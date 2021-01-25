package com.blueegg.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class LoginFailAdvanceResult() extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]{
  // 定义状态，保存当前所有的登陆失败事件，保存定时器的时间戳
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-state", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    // 首先判断事件类型
    if (value.eventType == "fail"){
      // 1.如果是失败，进一步做判断
      val iter = loginFailListState.get().iterator()
      // 判断之前是否有登陆失败事件
      if (iter.hasNext) {
        // 1.1 如果有，那么判断两次失败的时间差
        val firstFailEvent = iter.next()
        if (value.timestamp < firstFailEvent.timestamp + 2){
          // 如果在2秒之内，输出报警
          out.collect(LoginFailWarning(value.userId, firstFailEvent.timestamp, value.timestamp, "login fail 2 times in 2s"))
        }
        // 不管报不报警，当前都已处理完毕，将状态更新为最近一次登陆失败的事件
        loginFailListState.clear()
        loginFailListState.add(value)
      }else {
        // 1.2 如果没有，直接把当前事件添加到ListState中
        loginFailListState.add(value)
      }
    }else {
      // 2 如果是成功，直接清空状态
      loginFailListState.clear()
    }
  }
}
