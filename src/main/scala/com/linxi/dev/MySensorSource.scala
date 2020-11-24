package com.linxi.dev
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import scala.util.Random
/**
 * @ClassName: Kafka_Demo2
 * @Description: TODO
 * @author: linxi
 * @date: 2020/11/18  10:39
 */
object Kafka_Demo2 {
  def main(args: Array[String]): Unit = {
    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.api.scala._
    //获取kafka数据
    val kafkaDS: DataStream[SensorReading] = env.addSource(new MySensorSource())
    kafkaDS.print().setParallelism(1)
    env.execute()
  }
}

class MySensorSource() extends SourceFunction[SensorReading]{
  //flag表示数据是否还在正常运行
  var running:Boolean = true
  override def cancel(): Unit = {
    running = false
  }
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //初始化一个随机数发生器
    val random = new Random()
    val curTemp = 1.to(10).map(
      i => ("sensor_" + i, 65 + random.nextGaussian() * 20)
    )
    while (running){
      //更新温度值
      curTemp.map(
        t =>(t._1,t._2+random.nextGaussian())
      )
      //获取当前时间戳
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        t => sourceContext.collect(SensorReading(t._1,curTime,t._2))
      )
      Thread.sleep(100)
    }
  }
}
