package com.blueegg.hotitems_analysis

import org.apache.flink.api.common.functions.AggregateFunction


// 自定义预聚合函数AggregateFunction，下面中间这个参数表示中间状态，聚合状态就是当前商品的count值
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long]{
  override def createAccumulator(): Long = 0

  // 每来一条数据调用一次add， count值加一
  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}