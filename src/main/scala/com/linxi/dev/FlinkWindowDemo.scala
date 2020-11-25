package com.linxi.dev

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment

object FlinkWindowDemo {
  def main(args: Array[String]): Unit = {
    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置全局并行度
    env.setParallelism(1)
    //读取本地数据
    val input: DataStream[String] = env.readTextFile("sensor.txt")
    //获取tableEnv
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //val table: Any = tableEnv.fromDataStream(input, 'id, 'timestamp, 'temperature)(

    val sensorTable: Table = tableEnv.fromDataStream(input)
    //相关window操作

  }

}
