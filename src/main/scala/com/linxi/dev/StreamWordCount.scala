package com.linxi.dev

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @ClassName: StreamWordCount
 * @Description: TODO
 * @author: linxi
 * @date: 2020/11/17  10:27
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //创建流处理运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //接收socket文件流
    val socketDs: DataStream[String] = env.socketTextStream("flink129", 9999)

    //flatMap 和 map需要导入隐式转换
    import org.apache.flink.api.scala._
    val dataStream: DataStream[(String, Int)] = socketDs.flatMap(_.split("\\s"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    dataStream.print().setParallelism(1)

    //启动executor，执行任务
    env.execute("socket stream word count")
  }
}
