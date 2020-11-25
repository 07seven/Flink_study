package com.linxi.dev

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * @ClassName: Kafka_Demo1
 * @Description: TODO
 * @author: linxi
 * @date: 2020/11/18  10:22
 */
object Kafka_Demo1 {
  def main(args: Array[String]): Unit = {
    //配置kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "flink129:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.api.scala._
    //获取kafka数据
    val kafkaDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    kafkaDS.print().setParallelism(1)
    kafkaDS.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1).print().setParallelism(1)
    env.execute("kafka source")
  }
}
