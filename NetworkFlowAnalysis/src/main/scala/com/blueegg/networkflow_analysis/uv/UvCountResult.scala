package com.blueegg.networkflow_analysis.uv

import com.blueegg.networkflow_analysis.pageview.UserBehavior
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 自定义实现全窗口函数，用一个Set结构来保存所有的userId，进行自动去重
class UvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // 定义一个Set
    var userIdSet = Set[Long]()
    //遍历窗口中的所有数据，把userId添加到set中，自动去重
    for (userBehavior <- input) {
      userIdSet += userBehavior.userId
    }
    // 将set的size作为去重后的uv值输出
    out.collect(UvCount(window.getEnd, userIdSet.size))
  }
}
