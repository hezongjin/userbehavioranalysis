import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

case class LoginEvent(userId: Long, ip: String, flag: String, timestamp: Long)

case class LoginWarning(userId: Long, firstTime: Long, lastTime: Long, msg: String)

object LoginFailWithCep {

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

    val loginFailPattern = Pattern
      .begin[LoginEvent]("firstFail").where(_.flag.equals("fail"))
      .next("secondFail").where(_.flag.equals("fail"))
      //      .next("thirdFail").where(_.flag.equals("fail"))
      .within(Time.seconds(2))

    CEP.pattern(inputStream.keyBy(_.userId), loginFailPattern)
      .select(new PatternSelect())
      .print()

    env.execute()
  }
}

class PatternSelect() extends PatternSelectFunction[LoginEvent, LoginWarning] {

  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginWarning = {

    val firstFail = map.get("firstFail").get(0)
    val secondFail = map.get("secondFail").iterator().next()

    LoginWarning(firstFail.userId, firstFail.timestamp, secondFail.timestamp, "login fail")
  }
}