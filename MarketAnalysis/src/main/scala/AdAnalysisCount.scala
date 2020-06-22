import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AdAnalysisCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val resource = getClass.getResource("/AdClickLog.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        AdClickLog(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val resultStream = dataStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdByProvinceResult())

    resultStream.print()
    env.execute("AdAnalysisCount job")
  }
}

case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class CountByProvince(windowEnd: String, province: String, count: Long)

class AdCountAgg() extends AggregateFunction[AdClickLog,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdByProvinceResult() extends WindowFunction[Long ,CountByProvince,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(formatTs(window.getEnd),key,input.head))
  }

  private def formatTs (ts: Long) = {
    val df = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
    df.format (new Date (ts) )
  }

}