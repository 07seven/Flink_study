package com.linxi.dev

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * @ClassName: FlinkSqlDemo
 * @Description: TODO
 * @author: linxi
 * @date: 2020/11/19  10:11
 */
object FlinkSqlDemo {
  def main(args: Array[String]): Unit = {

    //流处理模式
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useOldPlanner() //使用老版本的planner
      .inStreamingMode() //流处理模式
      .build()

    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //批处理模式
    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val batchSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()

    TableEnvironment.create(batchSettings)

    //连接CSV
    tableEnvironment.connect(new FileSystem().path("sensor.txt")) //定义表数据来源，外部连接
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      ) //定义表结构
      .createTemporaryTable("inputTable") //创建临时表

    //连接kafka
    tableEnvironment.connect(
      new Kafka()
        .version("0.11") //定义kafka的版本
        .topic("sensor") //定义主题
        .property("zookeeper.connect", "flink129:2181")
        .property("bootstrap.servers", "flink129:9092")
    )
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

    //连接hive，hive source
    //注册catalog，使用hivename的catalog
    val hiveName = "myHive" //定义一个唯一的catalog名称，
    val defaultDataBase = "sdata" //默认数据库名称
    val hiveConfDir = "/opt/install/soft/hive/conf" //hive-site.xml 的路径
    val version = "3.1.2" //hive的版本号
    val catalog: HiveCatalog = new HiveCatalog(hiveName, defaultDataBase, hiveConfDir, version)

    tableEnvironment.registerCatalog(hiveName, catalog)
    tableEnvironment.useCatalog(hiveName)
    //创建数据库，目前不支持创建hive表
    val createDbSql = "create database if not exists myHive.test"
    //执行sql
    tableEnvironment.sqlUpdate(createDbSql)

    //tableAPI的使用
    val table: Table = tableEnvironment.from("kafkaInputTable")
    val resultTable: Table = table.select("id,temperature").filter("id=sensor_1")

    //sql查询
    val resultSqlTable: Table = tableEnvironment.sqlQuery("select id,temperature from kafkaInputTable where id = 'sensor_1'")

    //or
    val resultSqlTable2: Table = tableEnvironment.sqlQuery(
      """
        |select
        |id,temperature
        |from
        |kafkaInputTable
        |where
        |id = 'sensor_1'
        |""".stripMargin)

    //dataStream转换成表
    val inputStream: DataStream[String] = env.readTextFile("sensor.txt")

    import org.apache.flink.api.scala._
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val arr: Array[String] = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    val sensorTable: Table = tableEnvironment.fromDataStream(dataStream)

    //val sensorTable2: Table =tableEnvironment.fromDataStream(dataStream,"id, timestamp as ts")

    //根据dataStream创建视图
    tableEnvironment.createTemporaryView("sensorView", dataStream)
    //tableEnvironment.createTemporaryView("sensorView",dataStream,'id,'temperature,'timestamp as 'ts)

    //根据table创建视图
    tableEnvironment.createTemporaryView("sensorView", sensorTable)

    //表的输出，通常是将数据写入TableSink来实现的，TableSink是一个通用的接口，可以支持不同的文件格式，存储数据库和消息队列等
    tableEnvironment.connect(
      new FileSystem().path("sensor.txt")
    )
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE())) //定义表结构
      .createTemporaryTable("outputTable") //创建临时表

    resultSqlTable.insertInto("outputTable")

    //更新模式 uodate mode
    //追加模式append mode
    //在追加模式下，表（动态表）和外部连接器只交换插入（insert）信息
    //撤回模式 retract mode
    //在撤回模式下，表和外部连接器交换的是：添加（add）和撤回（Retract）消息
    /**
     * 插入（Insert）会被编码为添加消息；
     * 删除（Delete）则编码为撤回消息；
     * 更新（Update）则会编码为，已更新行（上一行）的撤回消息，和更新行（新行）的添加消息。
     * 在此模式下，不能定义key，这一点跟upsert模式完全不同。
     */
    //更新插入模式 upsert
    /**
     * 在Upsert模式下，动态表和外部连接器交换Upsert和Delete消息。
     * 这个模式需要一个唯一的key，通过这个key可以传递更新消息。为了正确应用消息，外部连接器需要知道这个唯一key的属性。
     * 插入（Insert）和更新（Update）都被编码为Upsert消息；
     * 删除（Delete）编码为Delete信息。
     * 这种模式和Retract模式的主要区别在于，Update操作是用单个消息编码的，所以效率会更高。
     */

    //输出到kafka
    tableEnvironment.connect(
      new Kafka()
        .version("0.11")
        .topic("sinkTest")
        .property("zookeeper.connect", "localhost:2181")
        .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaOutputTable")

    resultTable.insertInto("kafkaOutputTable")

    //输出到ElasticSearch
    //    tableEnvironment.connect(
    //      new Elasticsearch
    //    )

    //输出到mysql
    val sinkDDL: String =
      """
        |create table jdbcOutputTable(
        |id varchar(20) not null,
        |cnt bigint not null
        |)with(
        |'connector.type' = 'jdbc',
        |'connector.url' = 'jdbc:mysql://flink129:3306/test',
        |'connector.table' = 'sensor_count',
        |'connector.driver' = 'com.mysql.jdbc.Driver',
        |'connector.username' = 'root',
        |'connector.password' = '123456'
        |)
        |""".stripMargin

    tableEnvironment.sqlUpdate(sinkDDL)
    resultSqlTable.insertInto("jdbcOutputTable")


    //将表转换为DataStream\DataSet

    /**
     * Table API中表到DataStream有两种模式：
     * 追加模式（Append Mode）
     * 用于表只会被插入（Insert）操作更改的场景。
     * 撤回模式（Retract Mode）
     * 用于任何场景。有些类似于更新模式中Retract模式，它只有Insert和Delete两类操作。
     * 得到的数据会增加一个Boolean类型的标识位（返回的第一个字段），用它来表示到底是新增的数据（Insert），还是被删除的数据（老数据， Delete）。
     */

    val resultStream: DataStream[Row] = tableEnvironment.toAppendStream[Row](resultSqlTable)

    val resultStream2: DataStream[(Boolean, (Boolean, (String, Long)))] = tableEnvironment.toRetractStream[(Boolean, (String, Long))](resultSqlTable2)

    //query的解释和执行
    /**
     * explain方法会返回一个字符串，描述三个计划：
     * 未优化的逻辑查询计划
     * 优化后的逻辑查询计划
     * 实际执行计划
     */

    val explaination: String = tableEnvironment.explain(resultSqlTable)
    println(explaination)

    //流处理中的一些特殊概念
    /**
     * 1、流处理和关系代数（表、及sql）的区别
     * 2、动态表（Dynamic Tables）
     * 3、流式持续查询的过程
     *      1. 流被转换为动态表。
     *      2. 对动态表计算连续查询，生成新的动态表。
     *      3. 生成的动态表被转换回流。
     * 4.将流转换成表
     * 5、持续查询（Continuous Query）
     * 6、将动态表转换为流
     * 7、时间特性
     */

    //时间特性
    /**
     * 处理时间（Processing Time）
     * 1.DataStream转换为Table时候指定
     * proctime属性只能通过附加逻辑字段，来扩展物理schema。因此，只能在schema定义的末尾定义它。
     */
    //定义好DataStream
    val inputStream2: DataStream[String] = env.readTextFile("test.txt")
    val dataStream2: DataStream[SensorReading] = inputStream2.map(data => {
      val arr: Array[String] = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
    //将dataStream转换为Table，并指定时间字段
    val sensorTable2: Table = tableEnvironment.fromDataStream(dataStream2)

    /**
     * 处理时间（Processing Time）
     *定义TableSchema时候指定
     */
    tableEnvironment.connect(
      new FileSystem().path("..\\sensor.txt"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
        .field("pt", DataTypes.TIMESTAMP(3))
        .proctime()    // 指定 pt字段为处理时间
      ) // 定义表结构
      .createTemporaryTable("inputTable") // 创建临时表


    /**
     * 处理时间（Processing Time）
     *创建表的DDL时候指定
     * 注意：运行这段DDL，必须使用Blink Planner
     */

    val sinkDDL2: String =
      """
        |create table dataTable (
        |  id varchar(20) not null,
        |  ts bigint,
        |  temperature double,
        |  pt AS PROCTIME()
        |) with (
        |  'connector.type' = 'filesystem',
        |  'connector.path' = 'file:///D:\\..\\sensor.txt',
        |  'format.type' = 'csv'
        |)
  """.stripMargin

    tableEnvironment.sqlUpdate(sinkDDL2) // 执行 DDL

    /**
     * 事件时间（Event Time）
     * DataStream转换为Table时候指定
     */

  }

}
