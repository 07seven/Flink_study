package com.linxi.project

import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * 订单支付实时监控
 */
object OrderTimeoutWithoutCep {
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
      .keyBy(_.orderId)

    // 自定义一个 process function，进行order的超时检测，输出超时报警信息
    val timeoutWarningStream = orderEventStream.process(new OrderTimeoutAlert())
    timeoutWarningStream.print()

    env.execute("OrderTimeoutWithoutCep Job")

  }
}

class OrderTimeoutAlert() extends KeyedProcessFunction[Long,OrderEvent,OrderResult]{
  lazy val ispayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

  override def processElement(value: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {
    val isPayed = ispayedState.value()
    if (value.eventType == "create" && !isPayed) {
      context.timerService().registerEventTimeTimer(value.eventTime * 1000L + 15 * 60 * 1000L)
    } else if (value.eventType == "pay") {
      ispayedState.update(true)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    val isPayed = ispayedState.value()
    if (!isPayed) {
      out.collect(OrderResult(ctx.getCurrentKey, "order timeout"))
    }
    ispayedState.clear()
  }
}
