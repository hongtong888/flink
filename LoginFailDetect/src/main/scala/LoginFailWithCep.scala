import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val resource = getClass.getResource("/LoginLog.csv")
    val resourceStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.milliseconds((3000))) {
        override def extractTimestamp(element: LoginEvent): Long = {
          element.eventTime*1000L
        }
      })
    //定义匹配模式
    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("firstFail")
      .where(_.eventType == "fail")
      .next("secondFail")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))

    //在数据流中匹配出定义好的模式
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(resourceStream.keyBy(_.userId),pattern)

    //提取输出时间
    val dataStream = patternStream.select(new LoginFailDetect())

    dataStream.print("login fail with cep")
    env.execute("login fail with cep")
  }

}

class LoginFailDetect() extends PatternSelectFunction[LoginEvent,Warning]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    val firstFail = map.get("firstFail").get(0)
    val secondFail = map.get("secondFail").get(0)
    Warning(firstFail.userId,firstFail.eventTime,secondFail.eventTime,"login fail")
  }
}

