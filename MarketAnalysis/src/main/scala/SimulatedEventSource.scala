import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

class SimulatedEventSource extends RichParallelSourceFunction[MarketingUserBehavior]  {
  var running = true
  val channelSet: Seq[String] = Seq("AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba")
  val behaviorTypes: Seq[String] = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")
  val rand: Random = Random

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    val maxElements = Long.MaxValue
    var count = 0L

    while (running && count < maxElements) {
      val id = UUID.randomUUID().toString
      val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()

      ctx.collect(MarketingUserBehavior(id,behaviorType,channel,ts))
      count += 1
      TimeUnit.MILLISECONDS.sleep(5L)
    }

  }

  override def cancel(): Unit = running = false
}

case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)


