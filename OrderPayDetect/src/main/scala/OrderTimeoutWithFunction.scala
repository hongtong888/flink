import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderTimeoutWithFunction {
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
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(3)) {
        override def extractTimestamp(element: OrderEvent): Long = {
          element.eventTime*1000L
        }
      })
      .keyBy(_.orderId)
      .process(new OrderPayWithDetect())

    resourceStream.print("order timeout with function")
    resourceStream.getSideOutput(new OutputTag[OrderResult]("orderTimeOut")).print("timeout")
    env.execute("order timeout with function")
  }
}

class OrderPayWithDetect() extends KeyedProcessFunction[Long,OrderEvent,OrderResult]{

  //状态定义用来保存create pay 等时间标识  和时间戳状态标识
  lazy val isCreateState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-create",classOf[Boolean]))
  lazy val isPayState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-pay",classOf[Boolean]))
  lazy val tsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("time-ts",classOf[Long]))

  val orderTimeOutputTag = new OutputTag[OrderResult]("orderTimeOut")

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    //取出当前状态
    val isCreate = isCreateState.value()
    val isPay = isPayState.value()
    val ts = tsState.value()

    //根据当前事件的内类，分情况进行讨论
    //场景1：当前是create事件，对pay事件进行判断
    if(value.eventType == "create"){
      //如果成功支付
      if (isPay){
        out.collect(OrderResult(value.orderId,"order pay success"))
        isPayState.clear()
        tsState.clear()
        ctx.timerService().deleteEventTimeTimer(ts)
      }else{
        //没有支付  注册一个15分钟定时器
        val ts = value.eventTime*1000L + 15*60*1000L
        ctx.timerService().registerEventTimeTimer(ts)
        tsState.update(ts)
        isCreateState.update(true)
      }
    }else if (value.eventType == "pay"){
      //场景2：当前时间是pay事件，对create事件进行判断
      //如果已经支付过，匹配成功，判断时间间隔是否是15分钟之内
      if(isCreate){
        if (value.eventTime*1000L < ts){
          out.collect(OrderResult(value.orderId,"order pay success"))
        }else{
          //超时输出侧输出流
          ctx.output(orderTimeOutputTag,OrderResult(value.orderId,"payed but already timeout"))
        }
        isCreateState.clear()
        tsState.clear()
        ctx.timerService().deleteEventTimeTimer(ts)
      }else{
        //如果create没有来  需要等待乱序create 注册一个当前pay时间戳的定时器
        val ts = value.eventTime*1000L
        ctx.timerService().registerEventTimeTimer(ts)
        tsState.update(ts)
        isPayState.update(true)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      if (isPayState.value()){
        //如果pay过 那么说明create没有来 可能数据丢失
        ctx.output(orderTimeOutputTag,OrderResult(ctx.getCurrentKey,"payed but not found create log"))
      }else{
        //如果没有pay过，说明真正的15分钟超时了
        ctx.output(orderTimeOutputTag,OrderResult(ctx.getCurrentKey,"order time out"))
      }
    isPayState.clear()
    isCreateState.clear()
    tsState.clear()
  }
}