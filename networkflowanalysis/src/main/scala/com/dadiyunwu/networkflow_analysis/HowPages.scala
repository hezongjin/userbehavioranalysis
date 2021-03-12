package com.dadiyunwu.networkflow_analysis

import java.lang
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//输入数据样例类
case class ApachelogEvent(ip: String, userId: String, timestamp: Long, method: String, url: String)

case class PageViewCount(url: String, windowEnd: Long, count: Long)

object HowPages {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)

    val inputStream = env.readTextFile("E:\\learn\\userbehavioranalysis\\networkflowanalysis\\src\\main\\resources\\apache.log")

    val dataStream = inputStream.map(data => {
      val arr = data.split(" ")

      val ts = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(arr(3)).getTime
      ApachelogEvent(arr(0), arr(1), ts, arr(5), arr(6))
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApachelogEvent](Time.minutes(1)) {
        override def extractTimestamp(element: ApachelogEvent): Long = element.timestamp
      })


    val aggStream = dataStream
      //      .filter(_.url != null)
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(10))
      //      .aggregate(new CountAgg(), new WindowAgg())
      .process(new ProcessWindowFunction[ApachelogEvent, String, String, TimeWindow] {
      override def process(key: String, context: Context, elements: Iterable[ApachelogEvent], out: Collector[String]): Unit = {
        out.collect(key + ":" + elements.mkString(","))
      }
    })

    //    dataStream.print()
    aggStream.print()

    /*aggStream
      .keyBy(_.windowEnd)
      .process(new AggProcess(5))
      .print()*/


    env.execute()

  }

}

class CountAgg() extends AggregateFunction[ApachelogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: ApachelogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(a: Long, b: Long): Long = a + b
}

class WindowAgg() extends WindowFunction[Long, PageViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    val url = key
    val windowEnd = window.getEnd
    val count = input.iterator.next()

    out.collect(PageViewCount(url, windowEnd, count))
  }
}


class AggProcess(topSize: Int) extends KeyedProcessFunction[Long, PageViewCount, String] {

  var listState: ListState[PageViewCount] = null

  override def open(parameters: Configuration): Unit = {
    listState = getRuntimeContext.getListState(new ListStateDescriptor("listState", classOf[PageViewCount]))
  }

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {

    listState.add(value)

    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    val listValue = listState.get()
    listState.clear()

    val listBuffer = ListBuffer[PageViewCount]()
    val sb = new StringBuilder()
    sb.append("窗口结束时间:").append(timestamp - 1).append("\n")

    val iter = listValue.iterator()

    while (iter.hasNext) {
      listBuffer += iter.next()
    }

    val sortValue = listBuffer.sortBy(-_.count).take(topSize)

    for (elem <- sortValue.zipWithIndex) {
      sb.append("NO:" + elem._2 + 1)
        .append("\tURL:").append(elem._1.url)
        .append("\t热度:").append(elem._1.count)
        .append("\n")
    }

    sb.append("=" * 35)

    out.collect(sb.toString())
  }

}
