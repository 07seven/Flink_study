package com.linxi.project

import java.lang
import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * TODO:利用布隆过滤器实现uv统计
 */
object UvWithBloomFilter {
  def main(args: Array[String]): Unit = {
    val resourcePath: URL = getClass.getResource("/UserBehaviorTest.csv")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    import org.apache.flink.api.scala._
    env.readTextFile(resourcePath.getPath)
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserBehaviour(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behaviour == "pv")
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.minutes(60))
      .trigger(new MyTrigger()) //自定义窗口触发规则
      .process(new UvCountWithBloom()) //自定义窗口处理规则
      .print()

    env.execute("UvWithBloomFilter Job")
  }

  //自定义窗口触发规则
  class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
    override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.FIRE_AND_PURGE
    }

    override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {

    }
  }

  //自定义窗口处理规则
  class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
    //创建redis连接
    lazy val jedis: Jedis = new Jedis("localhost", 6397)
    lazy val bloom = new Bloom(1 << 29)

    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
      val storeKey: String = context.window.getEnd.toString
      var count = 0L
      if (jedis.hget("count", storeKey) != null) {
        count = jedis.hget("count", storeKey).toLong
      }
      val userId = elements.last._2.toString
      val offset = bloom.hash(userId, 61)
      val isExists: lang.Boolean = jedis.getbit(storeKey, offset)
      if(!isExists){
        jedis.setbit(storeKey, offset, true)
        jedis.hset("count", storeKey, (count + 1).toString)
        out.collect(UvCount(storeKey.toLong, count + 1))
      }else{
        out.collect(UvCount(storeKey.toLong, count))
      }
    }
  }
  // 定义一个布隆过滤器
  class Bloom(size: Long) extends Serializable {
    private val cap = size

    def hash(value: String, seed: Int): Long = {
      var result = 0
      for (i <- 0 until value.length) {
        // 最简单的hash算法，每一位字符的ascii码值，乘以seed之后，做叠加
        result = result * seed + value.charAt(i)
      }
      (cap - 1) & result
    }
  }
}
