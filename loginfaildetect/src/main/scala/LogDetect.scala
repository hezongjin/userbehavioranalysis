import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class LoginEvent(userId: Long, ip: String, flag: String, timestamp: Long)

case class LoginWarning(userId: Long, firstTime: Long, lastTime: Long, msg: String)

object LogDetect {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val path = this.getClass.getClassLoader.getResource("LoginLog.csv").getPath

    val inputStream = env.readTextFile(path)
      .map(data => {
        val arr = data.split(",")

        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000L
      })

    val dataStream = inputStream
      .keyBy(_.userId)
      .process(new LoginPro())

    dataStream.getSideOutput(new OutputTag[LoginWarning]("login-warning"))
      .print()

    env.execute()

  }
}

class LoginPro() extends KeyedProcessFunction[Long, LoginEvent, LoginEvent] {

  lazy val listState = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("listState", classOf[LoginEvent]))
  lazy val timerTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTs", classOf[Long]))


  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context, out: Collector[LoginEvent]): Unit = {

    if (value.flag.equals("fail")) {
      listState.add(value)

      if (timerTs.value() == 0) {
        val ts = value.timestamp * 1000 + 2000L
        ctx.timerService().registerEventTimeTimer(ts)
      }

    } else if (value.flag.equals("success")) {
      ctx.timerService().deleteEventTimeTimer(timerTs.value())
      timerTs.clear()
      listState.clear()
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext, out: Collector[LoginEvent]): Unit = {

    val loginEventBuffer = new ListBuffer[LoginEvent]()

    val iter = listState.get().iterator()
    while (iter.hasNext) {
      loginEventBuffer += iter.next()
    }

    if (loginEventBuffer.size >= 2) {
      ctx.output(new OutputTag[LoginWarning]("login-warning"), LoginWarning(loginEventBuffer.head.userId, loginEventBuffer.head.timestamp, loginEventBuffer.last.timestamp, ""))
    }

    listState.clear()
    timerTs.clear()

  }


}
