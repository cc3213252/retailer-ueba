package com.blueegg.market_analysis.ad

import org.apache.flink.api.common.functions.AggregateFunction

class AdCountAgg() extends AggregateFunction[AdClickLog, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickLog, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
