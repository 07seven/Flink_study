package com.linxi.project

import java.net.URL

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 订单支付实时监控
 * @param orderId
 * @param eventType
 * @param eventTime
 */

case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)
case class OrderResult(orderId: Long, eventType: String)


object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val resourcePath: URL = getClass.getResource("/OrderLog.csv")
    import org.apache.flink.api.scala._
    val orderEventStream : DataStream[OrderEvent] = env.readTextFile(resourcePath.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(3).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)

    // 定义一个带匹配时间窗口的模式
    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("follow")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 定义一个输出标签
    val orderTimeoutOutput = OutputTag[OrderResult]("orderTimeout")
    // 订单事件流根据 orderId 分流，然后在每一条流中匹配出定义好的模式
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream.keyBy("orderId"), orderPayPattern)
    val completedResult = patternStream.select(orderTimeoutOutput) {
      // 对于已超时的部分模式匹配的事件序列，会调用这个函数
      (pattern: scala.collection.Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val createOrder = pattern.get("begin")
        OrderResult(createOrder.get.iterator.next().orderId, "timeout")
      }
    }{
      // 检测到定义好的模式序列时，就会调用这个函数
      pattern: scala.collection.Map[String, Iterable[OrderEvent]] => {
        val payOrder = pattern.get("follow")
        OrderResult(payOrder.get.iterator.next().orderId, "success")
      }
    }
    // 拿到同一输出标签中的 timeout 匹配结果（流）
    val timeoutResult = completedResult.getSideOutput(orderTimeoutOutput)
    completedResult.print()
    timeoutResult.print()
    env.execute("Order Timeout Detect Job")
  }
}
