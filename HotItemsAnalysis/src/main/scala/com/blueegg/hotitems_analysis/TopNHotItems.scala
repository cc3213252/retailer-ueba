package com.blueegg.hotitems_analysis

import java.sql.Timestamp
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import scala.collection.mutable.ListBuffer


// 自定义KeyedProcessFunction
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
  // 先定义状态 ListState
  private var itemViewCountListState: ListState[ItemViewCount] = _


  override def open(parameters: Configuration): Unit = {
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-list", classOf[ItemViewCount]))

  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 每来一条数据，直接加入ListState
    itemViewCountListState.add(value)
    // 注册一个windowEnd + 1之后触发的定时器（加1毫秒，使之前的数据肯定到齐了，watermark过了windowEnd）
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 当定时器触发，可以认为所有窗口统计结果都已到齐，可以排序输出了
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 为了方便排序，另外定义一个ListBuffer，保存ListState里面的所有数据
    val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
    val iter = itemViewCountListState.get().iterator()
    while(iter.hasNext){
      allItemViewCounts += iter.next()
    }

    //清空状态
    itemViewCountListState.clear()

    //按照count大小排序，取前n个
    val sortedItemViewCounts = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //将排名信息格式化成String, 便于打印输出可视化展示
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)) // 前面加了1，这里减1

    // 遍历结果列表中的每个ItemViewCount，输出到一行
    for (i <- sortedItemViewCounts.indices) {
      val currentItemViewCount = sortedItemViewCounts(i)
      result.append("NO").append(i + 1).append(": ")
        .append("商品ID = ").append(currentItemViewCount.itemId).append("\t")
        .append("热门度 = ").append(currentItemViewCount.count).append("\n")
    }

    result.append("\n==============================\n\n")
    Thread.sleep(1000)
    out.collect(result.toString())
  }

}
