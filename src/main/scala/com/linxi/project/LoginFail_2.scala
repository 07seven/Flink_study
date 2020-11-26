package com.linxi.project

import java.net.URL
import java.util

import akka.event.Logging._
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


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

class  MatchFundtion() extends KeyedProcessFunction[Long,LoginEvent,Warning]{
  // 定义状态变量
  lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("saved login", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    // 首先按照type做筛选，如果success直接清空，如果fail再做处理
    if ( value.eventType == "fail" ){
      // 如果已经有登录失败的数据，那么就判断是否在两秒内
      val iter = loginState.get().iterator()
      if ( iter.hasNext ){
        val firstFail = iter.next()
        // 如果两次登录失败时间间隔小于2秒，输出报警
        if ( value.eventTime < firstFail.eventTime + 2 ){
          //out.collect(Warning( value.userId, firstFail.eventTime, value.eventTime, "login fail in 2 seconds." ) )
        }
        // 把最近一次的登录失败数据，更新写入state中
        val failList = new util.ArrayList[LoginEvent]()
        failList.add(value)
        loginState.update( failList )
      } else {
        // 如果state中没有登录失败的数据，那就直接添加进去
        loginState.add(value)
      }
    } else
      loginState.clear()
  }

}
