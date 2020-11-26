package com.linxi.project

import java.util.UUID
import java.util.concurrent.TimeUnit
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import scala.util.Random

/**
 * 自定义测试数据源
 * @param userId
 * @param behavior
 * @param channel
 * @param timestamp
 */

case class MarketingUserBehaviour(userId: Long, behavior: String, channel: String, timestamp: Long)
class SimulatedEventSource extends RichParallelSourceFunction[MarketingUserBehaviour]{
  var running = true
  val channelSet: Seq[String] = Seq("AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba")
  val behaviorTypes: Seq[String] = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")
  val random: Random.type = Random

  override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehaviour]): Unit = {
    val maxElements: Long = Long.MaxValue
    var count = 0L
    while (running && count < maxElements){
      val id = UUID.randomUUID().toString.toLong
      val behaviourType: String = behaviorTypes(random.nextInt(behaviorTypes.size))
      val chennel = channelSet(random.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()
      sourceContext.collectWithTimestamp(MarketingUserBehaviour(id,behaviourType,chennel,ts),ts)
      count += 1
      TimeUnit.MILLISECONDS.sleep(5L)
    }
  }
  override def cancel(): Unit = {
    running = false
  }
}
