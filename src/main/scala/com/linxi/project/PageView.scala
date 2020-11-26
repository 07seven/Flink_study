package com.linxi.project

import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * TODO:网站总浏览量pv的统计
 */
object PageView {
  def main(args: Array[String]): Unit = {
    val resourcePath: URL = getClass.getResource("/UserBehaviorTest.csv")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    import  org.apache.flink.api.scala._
    env.readTextFile(resourcePath.getPath)
      .map(line => {
        val arr: Array[String] = line.split(",")
        UserBehaviour(arr(0).toLong,arr(1).toLong,arr(2).toInt,arr(3),arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behaviour == "pv")
      .map(x => ("pv",1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(60*60))
      .sum(1)
      .print()

    env.execute("PageView Job")
  }
}

case class UserBehaviour(userId:Long, itemId:Long, categoryId:Int,  behaviour: String, timestamp:Long )
