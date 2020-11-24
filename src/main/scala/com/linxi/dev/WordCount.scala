package com.linxi.dev

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
 * @ClassName: WordCount
 * @Description: 单词统计
 * @author: linxi
 * @date: 2020/11/17  10:17
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //从文件中读取数据
    val inputPath="D:\\flink_test\\input1\\1.txt"
    val inputDS: DataSet[String] = env.readTextFile(inputPath)

    //分词之前，对单词进行group by分组，然后用sun进行聚合

    //导入隐式转换
    import org.apache.flink.api.scala._
    val wordCount: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    //打印结果
    wordCount.print()
  }

}
