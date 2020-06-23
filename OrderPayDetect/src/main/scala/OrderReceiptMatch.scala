import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderReceiptMatch {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val resourceOrder = getClass.getResource("/OrderLog.csv")
    val resourceOrderStream = env.readTextFile(resourceOrder.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderReceiptEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .filter(_.txId != "")
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderReceiptEvent](Time.seconds(3)) {
        override def extractTimestamp(element: OrderReceiptEvent): Long = {
          element.eventTime*1000L
        }
      })
      .keyBy(_.txId)

    val resourceReceipt = getClass.getResource("/ReceiptLog.csv")
    val resourceReceiptStream = env.readTextFile(resourceReceipt.getPath)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(3)) {
        override def extractTimestamp(element: ReceiptEvent): Long = {
          element.eventTime*1000L
        }
      })
      .keyBy(_.txId)

    //对两条流进行合并
    val processStream = resourceOrderStream
      .connect(resourceReceiptStream)
      .process(new TxMatchDetect)
    processStream.getSideOutput(new OutputTag[OrderReceiptEvent]("unmatchedPays")).print("unmatchedPays")
    processStream.getSideOutput(new OutputTag[ReceiptEvent]("unmatchedReceipt")).print("unmatchedReceipt")

    processStream.print("order receipt match")
    env.execute("order receipt match")
  }
}

case class OrderReceiptEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )
case class ReceiptEvent( txId: String, payChannel: String, eventTime: Long )

class TxMatchDetect extends CoProcessFunction[OrderReceiptEvent,ReceiptEvent, (OrderReceiptEvent,ReceiptEvent)]{
  lazy val payState: ValueState[OrderReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderReceiptEvent]("pay-state",classOf[OrderReceiptEvent]))
  lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state",classOf[ReceiptEvent]))

  override def processElement1(pay: OrderReceiptEvent, ctx: CoProcessFunction[OrderReceiptEvent, ReceiptEvent, (OrderReceiptEvent, ReceiptEvent)]#Context, out: Collector[(OrderReceiptEvent, ReceiptEvent)]): Unit = {
     //pay数据来的操作
    val receipt = receiptState.value()
    if (receipt!= null){
      //如果receipt状态存在，则正常输出
      out.collect((pay,receipt))
      receiptState.clear()
    }else{
      //如果状态不存在,注册一个定时器等状态的到来
      payState.update(pay)
      ctx.timerService().registerEventTimeTimer(pay.eventTime*1000L + 5000L)
    }

  }

  override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderReceiptEvent, ReceiptEvent, (OrderReceiptEvent, ReceiptEvent)]#Context, out: Collector[(OrderReceiptEvent, ReceiptEvent)]): Unit = {
     //receipt数据来的操作
    val pay = payState.value()
    if(pay!=null){
      out.collect((pay,receipt))
      payState.clear()
    }else{
      receiptState.update(receipt)
      ctx.timerService().registerEventTimeTimer(receipt.eventTime*1000L + 3000L)
    }

  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderReceiptEvent, ReceiptEvent, (OrderReceiptEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderReceiptEvent, ReceiptEvent)]): Unit = {
    if (payState.value()!=null){
      ctx.output(new OutputTag[OrderReceiptEvent]("unmatchedPays"),payState.value())
    }
    if (receiptState.value()!=null){
      ctx.output(new OutputTag[ReceiptEvent]("unmatchedReceipt"),receiptState.value())
    }
    payState.clear()
    receiptState.clear()
  }
}