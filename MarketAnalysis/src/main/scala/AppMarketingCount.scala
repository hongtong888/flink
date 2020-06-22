
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AppMarketingCount {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val dataStream = env.addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.timestamp)

    val resultStream = dataStream
      .filter(_.behavior != "UNINSTALL")
      .map(data => ("total",1L))
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new MarketCountAgg(),new MarketCountResult())

    resultStream.print()
    env.execute("AppMarketingCount job")
  }
}

class MarketCountAgg() extends AggregateFunction[(String,Long),Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator +1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class MarketCountResult() extends WindowFunction[Long, MarketingCountView,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingCountView]): Unit = {
    val startTs = window.getStart
    val endTs = window.getEnd
    val count = input.head
    out.collect( MarketingCountView(formatTs(startTs), formatTs(endTs), "total", "total", count) )
  }
  private def formatTs (ts: Long) = {
    val df = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
    df.format (new Date (ts) )
  }
}