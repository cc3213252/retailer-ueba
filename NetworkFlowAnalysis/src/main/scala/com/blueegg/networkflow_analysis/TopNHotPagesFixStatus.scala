package com.blueegg.networkflow_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class TopNHotPagesFixStatus(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {
  lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long]))

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
    pageViewCountMapState.put(value.url, value.count)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    // 另外注册一个定时器，1分钟之后触发，这时窗口已经彻底关闭，不再有聚合结果输出，可以清空状态
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 判断定时器触发时间，如果已经是窗口结束时间1分钟之后，那么直接清空状态
    if (timestamp == ctx.getCurrentKey + 60000L) {
      pageViewCountMapState.clear()
      return
    }

    val allPageViewCounts: ListBuffer[(String, Long)] = ListBuffer()
    val iter = pageViewCountMapState.entries().iterator()
    while(iter.hasNext) {
      val entry = iter.next()
      allPageViewCounts += ((entry.getKey, entry.getValue))  // 外面的括号表示方法调用，里面括号表示二元祖
    }

    // 按照访问量排序并输出top n
    val sortedPageViewCounts = allPageViewCounts.sortWith(_._2 > _._2).take(n)

    //将排名信息格式化成String, 便于打印输出可视化展示
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)) // 前面加了1，这里减1

    // 遍历结果列表中的每个ItemViewCount，输出到一行
    for (i <- sortedPageViewCounts.indices) {
      val currentItemViewCount = sortedPageViewCounts(i)
      result.append("NO").append(i + 1).append(": ")
        .append("页面URL = ").append(currentItemViewCount._1).append("\t")
        .append("热门度 = ").append(currentItemViewCount._2).append("\n")
    }

    result.append("\n==============================\n\n")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
