package com.blueegg.networkflow_analysis.pageview

import org.apache.flink.api.common.functions.AggregateFunction

// 定义预聚合函数
class PvCountAgg() extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  // 每来一条数据调用一次add， count值加一
  override def add(in: (String, Long), acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
