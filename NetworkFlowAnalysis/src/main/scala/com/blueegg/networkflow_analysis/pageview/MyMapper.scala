package com.blueegg.networkflow_analysis.pageview

import org.apache.flink.api.common.functions.MapFunction

import scala.util.Random

// 自定义mapper， 随机生成分组的key
class MyMapper() extends MapFunction[UserBehavior, (String, Long)]{
  override def map(t: UserBehavior): (String, Long) = {
    (Random.nextString(10), 1L)
  }
}
