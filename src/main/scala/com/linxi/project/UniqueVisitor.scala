package com.linxi.project

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * TODO:统计独立用户访问次数 UV
 */
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val resourcePath = getClass.getResource("/UserBehaviorTest.csv")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    import org.apache.flink.api.scala._
    env.readTextFile(resourcePath.getPath)
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserBehaviour(arr(0).toLong,arr(1).toLong,arr(2).toInt,arr(3),arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behaviour == "pv")
      .timeWindowAll(Time.seconds(60 * 60))
      .apply(new UvCountByWindow())
      .print()

    env.execute("UniqueVisitor Job")
  }

  class UvCountByWindow() extends AllWindowFunction[UserBehaviour,UvCount,TimeWindow]{
    override def apply(window: TimeWindow, input: Iterable[UserBehaviour], out: Collector[UvCount]): Unit = {
      val s: mutable.Set[Long] = collection.mutable.Set()
      var idSet = Set[Long]()
      for (userBehavior <- input){
        idSet += userBehavior.userId
      }
      out.collect(UvCount(window.getEnd,idSet.size))
    }
  }
}

case class UvCount(windowEnd:Long,count:Long)
