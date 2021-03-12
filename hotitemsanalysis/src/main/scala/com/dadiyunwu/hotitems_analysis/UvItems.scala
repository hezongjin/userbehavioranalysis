package com.dadiyunwu.hotitems_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


case class UvCount(windowEnd: Long, count: Long)

object UvItems {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val path = this.getClass.getClassLoader.getResource("").getPath
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
      //      .keyBy()
      .timeWindowAll(Time.hours(1))
      .apply(new AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {

          var userIdSet = Set[Long]()

          for (elem <- input) {
            userIdSet += (elem.userId)
          }

          out.collect(UvCount(window.getEnd, userIdSet.size))
        }
      })
      .print()


    env.execute()

  }

}
