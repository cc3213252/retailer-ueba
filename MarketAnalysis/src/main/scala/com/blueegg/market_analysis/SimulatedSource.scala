package com.blueegg.market_analysis

import java.util.UUID

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.util.Random

// 自定义测试数据源
class SimulatedSource() extends RichSourceFunction[MarketUserBehavior]{
  // 是否运行的标志位
  var running = true
  // 定义用户行为和渠道的集合
  val behaviorSet: Seq[String] = Seq("view", "download", "install", "uninstall")
  val channelSet: Seq[String] = Seq("appstore", "weibo", "wechat", "tieba")
  val rand: Random = Random

  override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    // 定义一个生成数据最大的数量
    val maxCounts = Long.MaxValue
    var count = 0L

    // while循环，不停地产生数据
    while(running && count < maxCounts) {
      val id = UUID.randomUUID().toString
      val behavior = behaviorSet(rand.nextInt(behaviorSet.size))  // 随机生成数据
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()

      ctx.collect(MarketUserBehavior(id, behavior, channel, ts))
      count += 1
      Thread.sleep(50L)
    }
  }

  override def cancel(): Unit = running = false
}
