import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object OrderTimeoutWithCEP {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val path = this.getClass.getClassLoader.getResource("data.txt").getPath

    val inputStream = env.readTextFile(path)
      .map(data => {
        val arr = data.split(",")

        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(0)) {
        override def extractTimestamp(element: OrderEvent): Long = element.timestamp * 1000L
      })
      .keyBy(_.orderId)

    val pattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType.equals("create"))
      .followedBy("pay").where(_.eventType.equals("pay"))
      .within(Time.minutes(15))

    val lateOrder = new OutputTag[OrderResult]("lateOrder")
    val cepStream = CEP.pattern(inputStream, pattern)
      .select(
        lateOrder,
        new TimeOutSelect(),
        new Select()
      )

    cepStream.print("success")
    cepStream.getSideOutput(lateOrder).print("late")

  }

  class TimeOutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
    override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
      val orderId = map.get("create").iterator().next().orderId
      OrderResult(orderId, s"${orderId} is time out")
    }
  }

  class Select() extends PatternSelectFunction[OrderEvent, OrderResult] {
    override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
      val orderId = map.get("pay").iterator().next().orderId
      OrderResult(orderId, s"${orderId} is success")
    }
  }

}
