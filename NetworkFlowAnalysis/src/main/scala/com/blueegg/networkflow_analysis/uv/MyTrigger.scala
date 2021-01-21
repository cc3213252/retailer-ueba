package com.blueegg.networkflow_analysis.uv

import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

// 触发器，每来一条数据，直接触发窗口计算并清空窗口状态
class MyTrigger() extends Trigger[(String, Long), TimeWindow]{
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}
