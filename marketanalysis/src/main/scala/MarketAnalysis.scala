import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

case class MarketResult(windowStart: String, windowEnd: String, behavior: String, channel: String, count: Long)

object MarketAnalysis {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new MySource)
      .filter(!_.behavior.equals("uninstall"))
      .keyBy(data => (data.behavior, data.channel))
      .timeWindow(Time.days(1))
      .trigger(new MyTrigger)
      .process(new MyProcess())
      .print()


    env.execute()
  }

}

class MySource extends SourceFunction[MarketUserBehavior] {

  var flag = true
  val behaviorSet = Array("view", "download", "install", "uninstall")
  val channelSet = Array("appstore", "weibo", "wechat", "tieba")

  override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {

    while (flag) {
      val id = UUID.randomUUID().toString
      val behavior = behaviorSet(Random.nextInt(behaviorSet.size))
      val channel = channelSet(Random.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()

      ctx.collect(MarketUserBehavior(id, behavior, channel, ts))

      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = {
    flag = false
  }
}

class MyProcess() extends ProcessWindowFunction[MarketUserBehavior, MarketResult, (String, String), TimeWindow] {

  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketResult]): Unit = {

    val start = new Timestamp(context.window.getStart).toString
    val end = new Timestamp(context.window.getEnd).toString
    val behavior = key._1
    val channel = key._2
    val count = elements.size
    out.collect(MarketResult(start, end, behavior, channel, count))
  }

}

class MyTrigger extends Trigger[MarketUserBehavior, TimeWindow] {
  override def onElement(element: MarketUserBehavior, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}