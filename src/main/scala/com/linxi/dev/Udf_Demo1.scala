package com.linxi.dev

import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @ClassName: Udf_Demo1
 * @Description: TODO
 * @author: linxi
 * @date: 2020/11/18  13:53
 */
object Udf_Demo1 {
  def main(args: Array[String]): Unit = {
    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //从调用开始给env创建一个stream追加时间特性
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //导入隐式转换
    import org.apache.flink.api.scala._
    //获取数据
    val dataStream: DataStream[String] = env.fromElements("spark", "kafka", "hadoop", "flink")
    //filter 实现filterFunction接口
    val filterDS: DataStream[String] = dataStream.filter(new FlinkFilter)
    //匿名函数实验
    val richDS: DataStream[String] = dataStream.filter(new RichFilterFunction[String] {
      override def filter(t: String): Boolean = {
        t.contains("spark")
      }
    })
    //匿名函数，lambda表达式
    val lambdaDS: DataStream[String] = dataStream.filter(_.contains("hadoop"))
    richDS.print().setParallelism(1)
    filterDS.print().setParallelism(1)
    lambdaDS.print().setParallelism(1)
    env.execute()
  }
}
class FlinkFilter extends FilterFunction[String]{
  override def filter(t: String): Boolean = {
    t.contains("flink")
  }
}