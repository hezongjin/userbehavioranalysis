package com.dadiyunwu.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import com.dadiyunwu.hotitems_analysis.util.FlinkHelper
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.mutable.ListBuffer

object test2 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 30 * 1000L))

    val stateBackend: StateBackend = new RocksDBStateBackend("file:///D:\\MyCk\\Test2", true)
    env.setStateBackend(stateBackend)

    env.enableCheckpointing(60 * 1000L)
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointTimeout(60 * 1000L)
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    checkpointConfig.setTolerableCheckpointFailureNumber(3)
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val prop = new Properties()
    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dev01:9092")
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer")
    prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    val consumer = new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), prop).setStartFromEarliest()

    val inputStream = env.addSource(consumer)


    val dataStream = inputStream.map(data => {
      val arr = data.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(3)) {
        override def extractTimestamp(t: UserBehavior): Long = t.timestamp * 1000
      })

    val aggStream = dataStream
      .filter(_.behavior.equals("pv"))
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAggTest4(), new WindowAggTest4())


    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process(new AggProcess(5))

    inputStream.print("data")
    aggStream.print("agg")
    resultStream.print()


    //    env.execute()
    val ckPath = "file:///D:\\MyCk\\Test2\\c90c4d41aa48a93646415b8d2fc518bb\\chk-1"
    FlinkHelper.start(env, ckPath)
  }
}

class CountAggTest4() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowAggTest4() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {

    val itemId = key
    val windowEnd = window.getEnd
    val count = input.iterator.next()

    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

class AggProcess(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  var listState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    listState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("listState", classOf[ItemViewCount]))
  }

  override def processElement(elem: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {

    listState.add(elem)

    context.timerService().registerEventTimeTimer(elem.windowEnd + 1)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    val listStateValue = listState.get().iterator()
    val listBuffer = ListBuffer[ItemViewCount]()

    while (listStateValue.hasNext) {
      listBuffer += listStateValue.next()
    }
    listState.clear()

    val counts = listBuffer.sortBy(-_.cound).take(topSize)

    val sb = new StringBuilder()
    sb.append("窗口结束时间:").append(new Timestamp(timestamp - 1)).append("\n")

    for (elem <- counts.zipWithIndex) {
      sb.append("NO:").append(elem._2)
        .append("\t商品ID:").append(elem._1.itemId)
        .append("\t商品热度:").append(elem._1.cound)
        .append("\n")
    }

    sb.append("=" * 35)

    out.collect(sb.toString())
  }
}