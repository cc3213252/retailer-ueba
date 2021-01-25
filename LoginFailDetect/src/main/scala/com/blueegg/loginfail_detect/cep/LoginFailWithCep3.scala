package com.blueegg.loginfail_detect.cep

import com.blueegg.loginfail_detect.LoginEvent
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

// 5秒之内有3次失败报警
object LoginFailWithCep3 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

    val loginEventStream = inputStream
      .map(data => {
        val arr = data.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000
      })

    // 1. 定义匹配的模式，要求是一个登陆失败事件后，紧跟另一个登陆失败事件
    val loginFailPattern = Pattern
      .begin[LoginEvent]("fail").where(_.eventType == "fail").times(3).consecutive()
      .within(Time.seconds(5))

    // 2.将模式应用到数据流上，得到一个PatternStream
    val patternStream = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

    // 3.检出符合模式的数据流，需要调用select
    val loginFailWarningStream = patternStream.select(new LoginFailEventMatch3())

    loginFailWarningStream.print()
    env.execute("login fail with cep job")
  }
}
