package com.dadiyunwu.hotitems_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{StateDescriptor, StateTtlConfig, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class result(windowEnd: Long, count: Long)

object PvItems {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    //    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val path = this.getClass.getClassLoader.getResource("UserBehavior.csv").getPath

    val inputStream = env.readTextFile(path)

    val dataStream: DataStream[UserBehavior] = inputStream.map(data => {
      val dataArr = data.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(3)) {
        override def extractTimestamp(t: UserBehavior): Long = {
          t.timestamp * 1000L
        }
      })


    dataStream.map(data => {
      ("pv", 1L)
    })
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .aggregate(new PvAgg(), new PvWindowAgg())
      /*.keyBy(_.windowEnd)
      .process(new KeyedProcessFunction[Long, result, result] {
        lazy val state = getRuntimeContext.getState(new ValueStateDescriptor[Long]("state", classOf[Long]))

        override def processElement(value: result, ctx: KeyedProcessFunction[Long, result, result]#Context, out: Collector[result]): Unit = {
          state.update(state.value() + value.count)

          ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey + 1)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, result, result]#OnTimerContext, out: Collector[result]): Unit = {
          out.collect(result(ctx.getCurrentKey, state.value()))
          state.clear()
        }
      })*/
      .print()


    env.execute()

  }
}

class PvAgg() extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: (String, Long), acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class PvWindowAgg() extends WindowFunction[Long, result, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[result]): Unit = {
    out.collect(result(window.getEnd, input.head))
  }
}








