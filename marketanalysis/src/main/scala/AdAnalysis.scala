import java.lang
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class AdClickCountByProvince(windowEnd: String, province: String, count: Long)

case class Warning(userId: Long, AdId: Long, msg: String)

/** *
  * 风控 可以看一下
  */
object AdAnalysis {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val path = this.getClass.getClassLoader.getResource("AdClickLog.csv").getPath

    val inputStream = env.readTextFile(path)
      .map(data => {
        val arr = data.split(",")

        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[AdClickLog](Time.seconds(3)) {
        override def extractTimestamp(element: AdClickLog): Long = element.timestamp * 1000L
      })

    val filerStream = inputStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterProcess())


    filerStream
      .keyBy(_.province)
      .timeWindow(Time.days(1), Time.seconds(5))
      /* .process(new ProcessWindowFunction[AdClickLog, AdClickCountByProvince, String, TimeWindow] {

         override def process(key: String, context: Context, elements: Iterable[AdClickLog], out: Collector[AdClickCountByProvince]): Unit = {
           out.collect(AdClickCountByProvince(new Timestamp(context.window.getEnd).toString, key, elements.size))
         }
       })*/
      .aggregate(new Agg(), new WindowAgg())
      .print()

    filerStream
      .getSideOutput(new OutputTag[Warning]("warning"))
      .print("warning")

    env.execute()
  }

}

class Agg() extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickLog, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowAgg() extends WindowFunction[Long, AdClickCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdClickCountByProvince]): Unit = {
    out.collect(AdClickCountByProvince(new Timestamp(window.getEnd).toString, key, input.head))
  }
}

class FilterProcess() extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {

  lazy val countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("countState", classOf[Long]))
  lazy val timerTsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTsState", classOf[Long]))
  lazy val isBack = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isBack", classOf[Boolean]))

  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {

    val curCount = countState.value()

    //第一条数据来了 注册第二天凌晨的定时器
    if (curCount == 0) {
      val ts = (ctx.timestamp() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24) - 8 * 60 * 60 * 1000
      ctx.timerService().registerEventTimeTimer(ts)
      timerTsState.update(ts)
    }

    /*if (isBack.value()) {
      return
    } else {
      println(s"countState.value():${countState.value()}")
      if (countState.value() + 1 >= 100) {
        isBack.update(true)
      } else {
        countState.update(curCount + 1)
      }

    }
    out.collect(value)*/

    if (curCount >= 100) {
      if (!isBack.value()) {
        isBack.update(true)
        ctx.output(new OutputTag[Warning]("warning"), Warning(value.userId, value.adId, s"${value.userId} click ${value.adId} over 100 times"))
      }
      return
    }

    countState.update(curCount + 1)
    out.collect(value)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {

    if (timestamp == timerTsState.value()) {
      countState.clear()
      isBack.clear()
    }
  }

}