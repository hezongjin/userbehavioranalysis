import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

case class OrderResult(orderId: Long, resultMsg: String)

object OrderTimeout {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val path = this.getClass.getClassLoader.getResource("data.txt").getPath

    val inputStream = env.readTextFile(path)
      //      env.socketTextStream("dev01", 6666)
      .map(data => {
      val arr = data.split(",")

      OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(0)) {
        override def extractTimestamp(element: OrderEvent): Long = element.timestamp * 1000L
      })

    val dataStream = inputStream
      .keyBy(_.orderId)
      .process(new KeyedProcessFunction[Long, OrderEvent, OrderResult] {

        lazy val orderState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("order-state", classOf[Boolean]))
        //        lazy val payState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("pay-state", classOf[Boolean]))
        lazy val timeTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

        override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {

          /* if (value.eventType.equals("create")) {
             if (payState.value() == true) {
               payState.clear()
               ctx.timerService().deleteEventTimeTimer(timeTs.value())
               timeTs.clear()
             } else {
               orderState.update(true)
               ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 15 * 60 * 1000L)
               timeTs.update(value.timestamp * 1000L + 1 * 60 * 1000L)
             }
           }

           if (value.eventType.equals("pay")) {
             if (orderState.value() == true) {
               orderState.clear()
               ctx.timerService().deleteEventTimeTimer(timeTs.value())
               timeTs.clear()
             } else {
               payState.update(true)
               ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + 15 * 60 * 1000L)
               timeTs.update(value.timestamp * 1000L + 1 * 60 * 1000L)
             }
           }*/


          if (value.eventType.equals("create")) {
            orderState.update(true)
            ctx.timerService().registerEventTimeTimer((value.timestamp * 1000 + 15 * 60 * 1000L))
            timeTs.update((value.timestamp * 1000 + 15 * 60 * 1000L))
            return
          }

          if (value.eventType.equals("pay")) {
            ctx.timerService().deleteEventTimeTimer(timeTs.value())
            orderState.clear()
            timeTs.clear()
            return
          }

        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {

          //          println("key" + ctx.getCurrentKey)
          if (orderState.value() == true) {
            ctx.output(new OutputTag[String]("timeOutOrder"), ctx.getCurrentKey.toString)
          }

          orderState.clear()
          //          payState.clear()
          timeTs.clear()
        }

      })

    dataStream
      .getSideOutput(new OutputTag[String]("timeOutOrder"))
      .print("lateOrder")

    env.execute()

  }

}
