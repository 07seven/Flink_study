package com.linxi.dev


import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.mutable

/**
 * @ClassName: TumblingEventTimeWindows
 * @Description: TODO
 * @author: linxi
 * @date: 2020/11/18  14:42
 */
object TumblingEventTimeWindow {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置EventTime属性
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并行度
    env.setParallelism(1)
    //获取数据
    val dataStream: DataStream[String] = env.socketTextStream("flink129", 9999)
    //导入隐式准换
    import org.apache.flink.api.scala._
    //处理数据
    val textWithTsDstream: DataStream[(String, Long, Int)] = dataStream.map {
      text =>
        val strArr: Array[String] = text.split(" ")
        (strArr(0), strArr(1).toLong, 1)
    }
    val textWithEventTimeDstream: DataStream[(String, Long, Int)] = textWithTsDstream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.milliseconds(1000)) {
      override def extractTimestamp(t: (String, Long, Int)): Long = {
        return t._2
      }
    })
    val testKeyStream: KeyedStream[(String, Long, Int), Tuple] = textWithEventTimeDstream.keyBy(0)

    testKeyStream.print("testKey:")

    val windowStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = testKeyStream.window(TumblingEventTimeWindows.of(Time.seconds(2)))

    val groupDataStream: DataStream[mutable.HashSet[Long]] = windowStream.fold(new mutable.HashSet[Long]()) { case (set, (key, ts, count)) => set += ts }

    groupDataStream.print("window::::").setParallelism(1)

    env.execute()
  }
}
