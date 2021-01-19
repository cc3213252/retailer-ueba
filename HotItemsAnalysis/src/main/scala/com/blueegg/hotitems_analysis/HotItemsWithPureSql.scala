package com.blueegg.hotitems_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, _}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row


object HotItemsWithPureSql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.readTextFile("/Users/cyd/Desktop/study/flink/retailer-ueba/HotItemsAnalysis/src/main/resources/UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L) // 秒转换成毫秒

    // 定义表执行环境
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 纯sql实现
    tableEnv.createTemporaryView("dataTable", dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select *
        |from (
        |  select
        |    *,
        |    row_number()
        |      over (partition by windowEnd order by cnt desc)
        |      as row_num
        |  from (
        |     select
        |        itemId,
        |        hop_end(ts, interval '5' minute, interval '1' hour) as windowEnd,
        |        count(itemId) as cnt
        |     from dataTable
        |     where behavior = 'pv'
        |     group by
        |        itemId,
        |        hop(ts, interval '5' minute, interval '1' hour)
        |  ) )
        |where row_num <= 5
        |""".stripMargin)

    resultSqlTable.toRetractStream[Row].print()
    env.execute("hot items sql job")
  }
}
