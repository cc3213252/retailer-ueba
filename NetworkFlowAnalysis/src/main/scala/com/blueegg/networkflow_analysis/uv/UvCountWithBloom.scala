package com.blueegg.networkflow_analysis.uv

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

// 实现自定义的窗口处理函数
class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
  // 定义redis连接以及布隆过滤器
  lazy val jedis = new Jedis("localhost", 6379)
  lazy val bloomFilter = new Bloom(1<<29) // 位的个数： 2^6(64) * 2^20(1M) * 2^3(8bit), 64MB

  // 本来是收集齐所有数据，窗口触发计算的时候才会调用，现在每来一条数据都调用一次
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // 先定义redis中存储位图的key
    val storedBitMapKey = context.window.getEnd.toString

    // 另外将当前窗口的uv count值，作为状态保存到redis里，用一个叫做uvcount的hash表来保存（windowEnd, count）
    val uvCountMap = "uvcount"
    val currentKey = context.window.getEnd.toString
    var count = 0L
    // 从redis中取出当前窗口的uv count值
    if (jedis.hget(uvCountMap, currentKey) != null) {
      count = jedis.hget(uvCountMap, currentKey).toLong
    }
    // 去重，判断当前userId的hash值对应的位图位置，是否为0
    val userId = elements.last._2.toString
    // 计算hash值，就对应着位图中的偏移量
    val offset = bloomFilter.hash(userId, 61)
    // 用redis的位操作命令，取bitmap中对应位的值
    val isExist = jedis.getbit(storedBitMapKey, offset)
    if (!isExist) {
      // 如果不存在，那么位图对应位置置1，并且将count值加1
      jedis.setbit(storedBitMapKey, offset, true)
      jedis.hset(uvCountMap, currentKey, (count + 1).toString)
    }
  }
}
