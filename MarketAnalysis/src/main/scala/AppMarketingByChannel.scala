import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val dataStream = env.addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.timestamp)

    val resultStream = dataStream
      .filter(_.behavior != "UNINSTALL")
      .keyBy(data => (data.channel, data.behavior))
      .timeWindow(Time.hours(1), Time.seconds(5))
      .process(new MarketingCountByChannel())

    resultStream.print()
    env.execute("AppMarketing job")
  }
}

case class MarketingCountView(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

class MarketingCountByChannel extends ProcessWindowFunction[MarketingUserBehavior,MarketingCountView,(String,String),TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[MarketingUserBehavior], out: Collector[MarketingCountView]): Unit = {
    val startTs = context.window.getStart
    val endTs = context.window.getEnd
    val channel = key._1
    val behaviorType = key._2
    val count = elements.size
    out.collect( MarketingCountView(formatTs(startTs), formatTs(endTs), channel, behaviorType, count) )
  }

  private def formatTs (ts: Long) = {
    val df = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
    df.format (new Date (ts) )
  }

}