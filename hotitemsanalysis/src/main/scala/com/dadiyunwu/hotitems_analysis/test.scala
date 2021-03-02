package com.dadiyunwu.hotitems_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration.{ConfigOption, Configuration}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object test {
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

    val inputStream = env.readTextFile(this.getClass.getClassLoader.getResource("file2.csv").getPath)

    val dataStream = inputStream.map(data => {
      val arr = data.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(3)) {
        override def extractTimestamp(t: UserBehavior): Long = t.timestamp * 1000L
      })

    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior.equals("pv"))
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAggTest(), new WindowAggTest(5))

    aggStream.print()

    env.execute()
  }
}

class CountAggTest() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  //每来一条消息+1
  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowAggTest(topSize: Int) extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key
    val windowEnd = window.getEnd
    val count = input.iterator.next()

    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

