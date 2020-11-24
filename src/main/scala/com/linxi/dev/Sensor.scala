package com.linxi.dev

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @ClassName: Sensor
 * @Description: 从集合读取数据
 * @author: linxi
 * @date: 2020/11/18  10:09
 */



object Sensor {
  def main(args: Array[String]): Unit = {
    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    //集合数据
    val sensorDS: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_3", 1547718222, 34.3),
      SensorReading("sensor_5", 1547718444, 35.4),
      SensorReading("sensor_8", 1547718555, 28.3)
    ))
    sensorDS.print("stream1:").setParallelism(1)

    env.execute()

  }
}

//定义样例类，传感器id，时间戳，温度
case class SensorReading(id:String,timestamp:Long,temperature:Double)
