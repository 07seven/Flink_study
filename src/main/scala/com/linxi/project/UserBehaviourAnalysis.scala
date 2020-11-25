package com.linxi.project


import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object UserBehaviourAnalysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    import org.apache.flink.api.scala._
    //读取数据
    val dataStream: DataStream[String] = env
      .readTextFile("D:\\DataFile\\ProjectData\\flink\\UserBehavior.csv")
      .map(line =>{
          val arr: Array[String] = line.split(",")
           UserBehaviour(arr(0).toLong,arr(1).toLong,arr(2).toInt,arr(3),arr(4).toLong)
         })
      .filter(_.behaviour.equals("pv")) //过滤出pv事件
      .assignAscendingTimestamps(_.timestamp) //分配升序时间戳
      .keyBy(_.itemId) //由于需要统计热门商品，所以使用itemId来进行分流
      .timeWindow(Time.hours(1),Time.minutes(5))//每隔五分钟统计最近一小时的浏览次数
      //增量聚合和全窗口聚合结合使用
      //聚合结果ItemViewCount是每个窗口中每个商品被浏览的次数
      .aggregate(new CountAgg,new WindowResult)
      //对DataStream[ItemViewCount]使用窗口结束时间进行分流
      //每一条支流里面的元素都属于同一个窗口,元素是ItemViewCount
      //所以只需要对支流里面的元素按照count字段进行排序就可以了
      //支流里面的元素是有限的,因为都属于同一个窗口
      .keyBy(_.windowEnd)
      .process(new TopN(3))

    dataStream.print()
    env.execute()
  }


  class CountAgg extends AggregateFunction[UserBehaviour,Long,Long]{
    override def createAccumulator(): Long = 0L
    override def add(in: UserBehaviour, acc: Long): Long = acc + 1
    override def getResult(acc: Long): Long = acc
    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }


  //全窗口聚合函数
  class WindowResult extends  ProcessWindowFunction[Long,ItemViewCount,Long,TimeWindow]{
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key,context.window.getEnd,elements.head))
    }
  }


  class TopN(i: Int) extends  KeyedProcessFunction[Long,ItemViewCount,String]{
    //初始化一个列表状态的变量
    lazy val itemState:ListState[ItemViewCount] = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("item-state", Types.of[ItemViewCount])
    )
    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
      itemState.add(i)
      //由于所有value的windowEnd都一样，所以只会注册一个定时器
      context.timerService().registerEventTimeTimer(i.windowEnd + 100L)
    }
    //定时器用于排序
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      //导入隐式类型转换
      import scala.collection.JavaConversions._
      //将列表状态变量中的元素都转移到allItems中
      //因为列表转台变量没有排序的功能，所以必须取出来排序
      for (item <- itemState.get){
        allItems += item
      }
      //清空列表状态变量
      itemState.clear()
      //对allTimes降序排序
      val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(-_.count)
      //打印结果
      val result: StringBuilder = new StringBuilder
      result
        .append("============================\n")
        .append("窗口结束时间是：")
        .append(new Timestamp(timestamp - 100))
        .append("\n")
      for(i <- sortedItems.indices){
        val currItem: ItemViewCount = sortedItems(i)
        result
          .append("第")
          .append(i + 1)
          .append("名的商品id是：")
          .append(currItem.itemId)
          .append("，浏览量是：")
          .append(currItem.count)
          .append("\n")
      }
      result.append("============================\n")

      out.collect(result.toString())
    }
  }


}

case class UserBehaviour(
                        userId:Long,
                        itemId:Long,
                        categoryId:Int,
                        behaviour: String,
                        timestamp:Long
                        )

case class ItemViewCount(itemId:Long,//商品id
                         windowEnd:Long,//窗口结束时间
                         count:Long) //商品id在窗口结束时候浏览的次数
