package com.linxi.project

import java.net.URL

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 状态编程
 * @param userId
 * @param ip
 * @param eventType
 * @param eventTime
 */
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val resourcePath: URL = getClass.getResource("/LoginLog.csv")
    import org.apache.flink.api.scala._
    env
      .readTextFile(resourcePath.getPath)
      .map(line =>{
        val dataArray : Array[String] = line.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.milliseconds(3000)) {
        override def extractTimestamp(t: LoginEvent): Long = {
          t.eventTime * 1000L
        }
      })
      .keyBy(_.userId)
      .process(new MatchFundtion())
      .print()

    env.execute("LoginFail Job")
  }
}

class  MatchFundtion() extends KeyedProcessFunction[Long,LoginEvent,LoginEvent]{
  // 定义状态变量
  lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("saved login", classOf[LoginEvent]))

  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context, collector: Collector[LoginEvent]): Unit = {
    if (i.eventType == "fail"){
      loginState.add(i)
    }
  }

  //注册定时器，触发事件设定为2秒后
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext, out: Collector[LoginEvent]): Unit = {
    val allLogins: ListBuffer[LoginEvent] = ListBuffer()
    import scala.collection.JavaConversions._
    for (login <- loginState.get()){
      allLogins += login
    }
    loginState.clear()

    if (allLogins.length > 1){
      out.collect(allLogins.head)
    }
  }
}
