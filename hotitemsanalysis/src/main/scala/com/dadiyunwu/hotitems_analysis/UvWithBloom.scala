package com.dadiyunwu.hotitems_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UvWithBloom {

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

    dataStream
      .filter(_.behavior.equals("pv"))
      .map(data => ("uv", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger()) //增量聚合 没来一条数据 计算结果 清空数据
      .process(new UvCountWithBloom())

    env.execute()

  }

}

class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}

class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), result, String, TimeWindow] {

  //  lazy val jedis = new Jedis("121.36.90.108", 6379)
  lazy val jedis = new Jedis("mini2", 6379)
  lazy val bloomFilter = new Bloom(1 << 29) //1左位移29次,2的29次方

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[result]): Unit = {

    //bitMap的key值 就是windowEnd
    val bitMapKey = context.window.getEnd.toString

    val countKey = context.window.getEnd.toString

    val uvCount = "uvCount"

    val userID = elements.last._2.toString
    //userID的哈希值
    val offset = bloomFilter.hash(userID, 61)

    val isExist = jedis.getbit(bitMapKey, offset)

    if (!isExist) {
      val cnt = jedis.hget(uvCount, countKey)
      val newCount = if (cnt != null) {
        cnt.toLong + 1
      } else {
        1
      }
      //      println(s"uvCount:${uvCount},countkey:${countKey},newCount:${newCount}")
      jedis.hset(uvCount, countKey, newCount.toString)

      jedis.setbit(bitMapKey, offset, true)
    }

  }

}

//自定义一个布隆过滤器
class Bloom(size: Long) {

  private val cap = size

  def hash(value: String, seed: Int): Long = {

    var result = 0

    for (i <- 0 until value.size) {
      result = result * seed + value.charAt(i)
    }

    (cap - 1) & result
  }

}