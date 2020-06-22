import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 同一个用户2s之内连续登录失败
  */
object LoginFail {
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
      .keyBy(_.userId)
      .process(new LoginFailWarning(2))

    resourceStream.print("LoginFail")
    env.execute("LoginFail job")
  }

}

case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg:String)

class LoginFailWarning(maxFailTime: Int) extends KeyedProcessFunction[Long,LoginEvent,Warning]{
  //定义List状态，保存规定时间内的状态
  lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login state",classOf[LoginEvent]))
  //定义value状态，保存定时器的时间戳
  lazy val timeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time_ts",classOf[Long]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    //判断是否登录失败
    if (value.eventType == "fail"){
      //失败就添加到状态中，如果没注册过定时器就注册定时器
      loginState.add(value)
      if(timeState.value() == 0){
        val ts = value.eventTime*1000L + 2000L
        ctx.timerService().registerEventTimeTimer(ts)
        timeState.update(ts)
      }
    }else{
      //登录成功删除定时器 清除状态
      ctx.timerService().deleteEventTimeTimer(timeState.value())
      loginState.clear()
      timeState.value()
    }
  }
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    //如果2s内定时器触发了，判断ListState失败的个数
    val listBuffer = new ListBuffer[LoginEvent]
    val iter = loginState.get().iterator()
    while (iter.hasNext){
      listBuffer += iter.next()
    }
    if (listBuffer.size >= maxFailTime){
      out.collect(Warning(ctx.getCurrentKey,listBuffer.head.eventTime,listBuffer.last.eventTime,"login fail in 2s for "+listBuffer.size+" times"))
    }
    loginState.clear()
    timeState.clear()
  }
}
