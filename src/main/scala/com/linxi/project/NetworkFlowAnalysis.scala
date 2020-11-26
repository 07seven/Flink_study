package com.linxi.project


import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * TODO:实时流量统计
 */

object NetworkFlowAnalysis {
  def main(args: Array[String]): Unit = {
    //获取流运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置全局并行度
    env.setParallelism(1)
    //获取数据
    //导入隐式类型转换
    import org.apache.flink.api.scala._
    val stream = env
      .readTextFile("D:\\DataFile\\ProjectData\\flink\\apache.log")
      .map(line => {
        val arr: Array[String] = line.split(" ")
        val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp: Long = simpleDateFormat.parse(arr(3)).getTime
        ApacheLogEvent(arr(0), arr(2), timestamp, arr(5), arr(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds(1000)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = {
          t.eventTime
        }
      })
      .filter(
        data => {
          val pattern = "^((?!\\\\.(css|js)$).)*$".r
          (pattern findFirstIn data.url).nonEmpty
        }
      )
      .keyBy("url")
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new CountAgg(), new WindowRedultFunction())
      .keyBy(1)
      .process(new TopNHotUrls(5))
      .print
    env.execute("NetworkFlowAnalysis Job")
  }

  class CountAgg() extends AggregateFunction[ApacheLogEvent,Long,Long]{
    override def createAccumulator(): Long = 0L
    override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1
    override def getResult(acc: Long): Long = acc
    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class WindowRedultFunction() extends WindowFunction[Long,UrlViewCount,Tuple,TimeWindow]{
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      val url: String = key.asInstanceOf[Tuple1[String]]._1
      val count: Long = input.iterator.next()
      out.collect(UrlViewCount(url,window.getEnd,count))
    }
  }

  class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Tuple,UrlViewCount,String]{
    private var urlState: ListState[UrlViewCount]  = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      val urlStateDesc: ListStateDescriptor[UrlViewCount] = new ListStateDescriptor[UrlViewCount]("urlstate-state", classOf[UrlViewCount])
      val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(urlStateDesc)
    }

    override def processElement(input: UrlViewCount, context: KeyedProcessFunction[Tuple, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
      //每条数据都保存到状态中
      urlState.add(input)
      context.timerService().registerEventTimeTimer(input.windowEnd + 1)
    }

    //设置定时器
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      //获取收到的所有url的访问量
      val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (urlViews <- urlState.get){
        allUrlViews += urlViews
      }
      //提前清理状态中的数据，释放空间
      urlState.clear()
      //按照访问量从大到小排序
      val sortedUrlViews: ListBuffer[UrlViewCount] = allUrlViews.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
      //将排名信息格式化为String，便于打印
      val result: StringBuilder = new StringBuilder
      result.append("=================================\n")
      result
        .append("时间:")
        .append(new Timestamp(timestamp - 1))
        .append("\n")

      for(i <- sortedUrlViews.indices){
        val curretnUrlView: UrlViewCount = sortedUrlViews(i)
        // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
        result
          .append("NO")
          .append(i+1)
          .append(":")
          .append(" URL=")
          .append(curretnUrlView.url)
          .append(" 流量=")
          .append(curretnUrlView.count)
          .append("\n")
      }
      result.append("===============================\n")

      //控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString())
    }
  }
}

//输入的日志数据流
case class ApacheLogEvent(ip:String, userId:String, eventTime:Long, method:String, url:String)
//窗口操作统计的输出数据类型
case class UrlViewCount(url:String,windowEnd:Long,count:Long)
