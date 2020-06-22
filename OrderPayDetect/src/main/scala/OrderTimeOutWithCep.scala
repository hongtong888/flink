import java.util

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object OrderTimeOutWithCep {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val resource = getClass.getResource("/OrderLog.csv")
    val resourceStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.milliseconds((3000))) {
        override def extractTimestamp(element: OrderEvent): Long = {
          element.eventTime*1000L
        }
      })

    //定义匹配的模式
    val timeOutPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    //将pattern应用在按照orderId进行分组的数据流上
    val patternStream = CEP.pattern(resourceStream.keyBy(_.orderId),timeOutPattern)

    //定义一个侧输出流
    val orderTimeOutputTag = new OutputTag[OrderResult]("orderTimeOut")

    //匹配提取时间
   val resultStream = patternStream.select(orderTimeOutputTag,new OrderTimeOutSelect(),new OrderPaySelect())

    //输出
    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeOutputTag).print("time out ")

    env.execute("time out cep job")

  }
}

case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)
case class OrderResult(orderId: Long, eventType: String)

class OrderTimeOutSelect() extends PatternTimeoutFunction[OrderEvent,OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], timeoutTimeStamp: Long): OrderResult = {
    val orderId = map.get("create").iterator().next().orderId
    OrderResult(orderId,"time out "+ timeoutTimeStamp)
  }
}


class OrderPaySelect() extends PatternSelectFunction[OrderEvent,OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val orderId = map.get("pay").get(0).orderId
    OrderResult(orderId,"payed success")
  }
}

