package com.blueegg.market_analysis.ad

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object AdClickAnalysisFilter {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 从文件中读取数据
    val resource = getClass.getResource("/AdClickLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

    // 转换成样例类，并提取时间戳和watermark
    val adLogStream = inputStream
      .map(data => {
        val arr = data.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 插入一步过滤操作，并将有刷单行为的用户输出到侧输出流（黑名单报警）
    val filterBlackListUserStream: DataStream[AdClickLog] = adLogStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUserResult(100))

    // 开窗聚合统计
    val adCountResultStream = filterBlackListUserStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountWindowResult())

//    adCountResultStream.print("count result")
    filterBlackListUserStream.getSideOutput(new OutputTag[BlackListUserWarning]("warning")).print("warning")
    env.execute("ad count statistics job")
  }
}
