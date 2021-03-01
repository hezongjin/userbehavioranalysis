package com.dadiyunwu.hotitems_analysis


import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, cound: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    var env: StreamExecutionEnvironment = null
    try {
      env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
      init(env)
    } catch {
      case ex: Exception => {
        println(s"程序启动错误:${ex.getMessage}")
      }
    }

    val inputStream: DataStream[String] = env.readTextFile(this.getClass.getClassLoader.getResource("UserBehavior.csv").getPath)

    val dataStream: DataStream[UserBehavior] = inputStream.map(data => {
      val dataArr = data.split(",")
      UserBehavior(dataArr(0).toLong, dataArr(1).toLong, dataArr(2).toInt, dataArr(3), dataArr(4).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(3)) {
        override def extractTimestamp(t: UserBehavior): Long = {
          t.timestamp * 1000L
        }
      })


    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior.equals("pv"))
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg, new WindowAgg())

    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(5))

    resultStream.print()

    env.execute()

  }

  private def init(env: StreamExecutionEnvironment) = {
    env.setParallelism(8)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //作业重试次数,时间设置
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 60 * 1000L))

    //checkpoint
    env.enableCheckpointing(10 * 1000L)
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    checkpointConfig.setCheckpointTimeout(60 * 1000L)
    checkpointConfig.setTolerableCheckpointFailureNumber(3)
    //失败和取消程序时间 保留checkpoint数据 以便可以选择性的从某一个状态重启
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    val rocksDBStateBackend: StateBackend = new RocksDBStateBackend("file:///E:\\MyCk\\userbehavior", true)
    env.setStateBackend(rocksDBStateBackend)
  }
}

class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {

  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowAgg() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key
    val windowEnd = window.getEnd
    val count = input.iterator.next()

    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  var listState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    listState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemView-list", classOf[ItemViewCount]))
  }

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {

    listState.add(i)

    //设置1ms之后的timerService
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    val listBuffer = ListBuffer[ItemViewCount]()

    val iter = listState.get().iterator()

    while (iter.hasNext) {
      listBuffer += iter.next()
    }

    listState.clear()

    val sortListBuffer = listBuffer.sortBy(-_.cound).take(topSize)

    val stringBuilder = new StringBuilder


    /*//秒级别
    LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(8))
    //毫秒级别
    LocalDateTime.now().toInstant(ZoneOffset.ofHours(8)).toEpochMilli*/
    /*val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateTime = format.format(new Date(timestamp - 1))*/

    val dateTime = new java.sql.Timestamp(timestamp - 1)
    stringBuilder.append("窗口结束时间:").append(dateTime).append("\n")

    for (elem <- sortListBuffer.zipWithIndex) {
      stringBuilder.append("NO:" + (elem._2 + 1))
        .append("\t商品ID:" + elem._1.itemId)
        .append("\t商品热度:" + elem._1.cound)
        .append("\n")
    }
    stringBuilder.append("=" * 35)

    //    Thread.sleep(1000)

    out.collect(stringBuilder.toString())
  }
}
