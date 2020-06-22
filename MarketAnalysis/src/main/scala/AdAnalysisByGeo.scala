import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object AdAnalysisByGeo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val resource = getClass.getResource("/AdClickLog.csv")
    val resourceStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        AdClickLog(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //根据userId 和adId进行分组进行过滤。创建过程函数
    val filterStream = resourceStream
      .keyBy(data => (data.userId, data.userId))
      .process(new FilterBlackListUser(100))

    //根据过滤输出流在进行正常的计数操作
    val resultStream = filterStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdByProvinceResult())

    resultStream.print("result list")

    filterStream.getSideOutput(new OutputTag[BlackListWarning]("blacklist"))
      .print("black list")

    env.execute("AdAnalysisByGeo job")

  }
}

class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long,Long),AdClickLog,AdClickLog]{

  //保存当前用户对广告的点击量
  lazy val countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count_state",classOf[Long]))
  //保存当前用户是否在黑名单
  lazy val isSent = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is_sent",classOf[Boolean]))
  // 保存定时器触发的时间戳，届时清空重置状态
  lazy val resetTime = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-state", classOf[Long]))

  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    //获取点击量
    val curCount = countState.value()
    //如果是第一次点击注册个定时器，每天0点触发
    if (curCount == 0){
      val ts = (ctx.timerService().currentProcessingTime() / (24*60*60*1000) + 1) * (24*60*60*1000)
      resetTime.update(ts)
      ctx.timerService().registerProcessingTimeTimer(ts)
    }
    //如果计数超过上限则加入到黑名单中
    if (curCount > maxCount){
      if (!isSent.value()){
        isSent.update(true)
        //侧输出流进行输出
        ctx.output(new OutputTag[BlackListWarning]("blacklist"),BlackListWarning(value.userId, value.adId, "Click over " + maxCount + " times today."))
      }
    }
    countState.update(curCount+1)
    out.collect(value)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    if( timestamp == resetTime.value() ){
      isSent.clear()
      countState.clear()
    }

  }
}

case class BlackListWarning(userId: Long, adId: Long, msg: String)
