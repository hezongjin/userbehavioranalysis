package com.dadiyunwu.hotitems_analysis.tableapi

import java.util.Properties

import com.dadiyunwu.hotitems_analysis.UserBehavior
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

object Demo {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L))

    val stateBackend: StateBackend = new RocksDBStateBackend("file:///D:\\MyCk\\test", true)
    env.setStateBackend(stateBackend)

    env.enableCheckpointing(10 * 1000L)
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointTimeout(60 * 1000L)
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    checkpointConfig.setTolerableCheckpointFailureNumber(3)

    val prop = new Properties()
    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dev01:9092")
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer")
    prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    val consumer = new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), prop)
      .setStartFromEarliest()

    val inputStream = env.addSource(consumer)
    val dataStream = inputStream.map(data => {
      val arr = data.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(3)) {
        override def extractTimestamp(t: UserBehavior): Long = t.timestamp * 1000L
      })

    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()

    val tableEnv = StreamTableEnvironment.create(env, settings)

    val table = tableEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    val aggTable = table
      .filter('behavior === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
      .groupBy('itemId, 'sw)
      .select('itemId, 'sw.end as 'windowEnd, 'itemId.count as 'cnt)

    /*  tableEnv.createTemporaryView("aggTable", aggTable, 'itemId, 'windowEnd, 'cnt)

      tableEnv.sqlQuery(
        """
          |with t1 as(
          |select
          |  *,
          |  row_number() over (partition by windowEnd order by cnt desc) as row_num
          |from aggTable
          |)
          |select
          |*
          |from t1
          |where row_num <= 5
        """.stripMargin)
        .toRetractStream[Row]
        .print()*/

    tableEnv.createTemporaryView("dataTable", dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    tableEnv.sqlQuery(
      """
        |with t1 as(
        |select
        |  itemId,
        |  count(itemId) as cnt,
        |  hop_end(ts,interval '5' minute,interval '1' hour) as windowEnd
        |from dataTable
        |group by
        |  itemId,
        |  hop(ts,interval '5' minute,interval '1' hour)
        |),
        |t2 as (
        |select
        |  itemId,
        |  windowEnd,
        |  row_number() over(partition by windowEnd order by cnt desc) as row_num
        |from t1
        |)
        |select
        |  *
        |from t2
        |where row_num <= 5
      """.stripMargin)
      .toRetractStream[Row]
      .print()

    env.execute()
  }
}
