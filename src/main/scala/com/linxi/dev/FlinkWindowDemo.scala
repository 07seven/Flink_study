package com.linxi.dev

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, GroupWindow, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

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

    //需要导入隐式转换
    import org.apache.flink.table.api.scala._
    import org.apache.flink.api.scala._
    val sensorTable: Table = tableEnv.fromDataStream(input, 'id, 'timestamp as 'ts, 'temperature)
    //val sensorTable: Table = tableEnv.fromDataStream(input)
    //相关window操作
    val windowTable: Table = sensorTable.window(Tumble over 10.minutes on 'ts as 'w)
      .groupBy('id, 'w)
      .select('id, 'id.count)

    windowTable.toAppendStream[Row].print()

  }

}
