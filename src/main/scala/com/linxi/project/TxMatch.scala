package com.linxi.project

import java.net.URL

import com.linxi.project.TxMatch.{unmatchedPays, unmatchedReceipts}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )

case class ReceiptEvent( txId: String, payChannel: String, eventTime: Long )

object TxMatch {
  val unmatchedPays: OutputTag[OrderEvent] = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts: OutputTag[ReceiptEvent] = new OutputTag[ReceiptEvent]("unmatchedReceipts")
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val orderEventPath: URL = getClass.getResource("/OrderLog.csv")
    val receiptEventPath: URL = getClass.getResource("/ReceiptLog.csv")

    import org.apache.flink.api.scala._
    val orderEventStream: KeyedStream[OrderEvent, String] = env.readTextFile(orderEventPath.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    val receiptEventStream: KeyedStream[ReceiptEvent, String] = env.readTextFile(receiptEventPath.getPath)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    val processedStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream.connect(receiptEventStream).process(new TxMatchDetection())

    processedStream.getSideOutput(unmatchedPays).print("unmatchedPays")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")

    processedStream.print("processed")

    env.execute("TMatch Job")
  }
}

class TxMatchDetection() extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
  lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state",classOf[OrderEvent]) )
  lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]) )


  override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val receipt = receiptState.value()

    if( receipt != null ){
      receiptState.clear()
      out.collect((pay, receipt))
    } else{
      payState.update(pay)
      ctx.timerService().registerEventTimeTimer(pay.eventTime * 1000L)
    }
  }

  override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val payment = payState.value()

    if( payment != null ){
      payState.clear()
      out.collect((payment, receipt))
    } else{
      receiptState.update(receipt)
      ctx.timerService().registerEventTimeTimer(receipt.eventTime * 1000L)
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    if ( payState.value() != null ){
      ctx.output(unmatchedPays, payState.value())
    }
    if ( receiptState.value() != null ){
      ctx.output(unmatchedReceipts, receiptState.value())
    }
    payState.clear()
    receiptState.clear()
  }
}
