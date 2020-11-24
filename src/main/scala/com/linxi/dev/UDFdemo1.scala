package com.linxi.dev

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.{StreamTableEnvironment, tableConversions}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * @ClassName: UDFdemo1
 * @Description: TODO
 * @author: linxi
 * @date: 2020/11/19  16:57
 */
object UDFdemo1 {
  def main(args: Array[String]): Unit = {

    //系统内置函数
    /**
     * 比较函数
     * SQL：
     * value1 = value2
     * value1 > value2
     * Table API：
     * ANY1 === ANY2
     * ANY1 > ANY2
     * 逻辑函数
     * SQL：
     * boolean1 OR boolean2
     * boolean IS FALSE
     * NOT boolean
     * Table API：
     * BOOLEAN1 || BOOLEAN2
     * BOOLEAN.isFalse
     * !BOOLEAN
     * 算术函数
     * SQL：
     * numeric1 + numeric2
     * POWER(numeric1, numeric2)
     * Table API：
     * NUMERIC1 + NUMERIC2
     * NUMERIC1.power(NUMERIC2)
     * 字符串函数
     * SQL：
     * string1 || string2
     * UPPER(string)
     * CHAR_LENGTH(string)
     * Table API：
     * STRING1 + STRING2
     * STRING.upperCase()
     * STRING.charLength()
     * 时间函数
     * SQL：
     * DATE string
     * TIMESTAMP string
     * CURRENT_TIME
     * INTERVAL string range
     * Table API：
     * STRING.toDate
     * STRING.toTimestamp
     * currentTime()
     * NUMERIC.days
     * NUMERIC.minutes
     * 聚合函数
     * SQL：
     * COUNT(*)
     * SUM([ ALL | DISTINCT ] expression)
     * RANK()
     * ROW_NUMBER()
     * Table API：
     * FIELD.count
     * FIELD.sum0
     */

    //UDF：User-defined Functions函数
    /**
     *标量函数（Scalar Functions）
     */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    val inputStream: DataStream[String] = env.readTextFile("sensor.txt")
    import org.apache.flink.api.scala._
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //将dataStream转换为Table，并指定时间字段
   //tableEnv.fromDataStream(dataStream,'id,'timestamp.rowTime,'temperature)
   val sensorTable: Table = tableEnv.fromDataStream(dataStream)

    //TableAPI中使用
    val hashCode: HashCode = new HashCode(10)
//    sensorTable.select("id",hashCode("id"))

    //SQL中使用
    tableEnv.createTemporaryView("sensor",sensorTable)
    tableEnv.registerFunction("hashCode",hashCode)
    val resultSqlTable: Table = tableEnv.sqlQuery("select id,hashCode(id) from sensor")
    resultSqlTable.toAppendStream[Row].print("sql")

    env.execute()
  }
}

class HashCode(factor : Int) extends ScalarFunction{
  def eval(s : String) : Int = {
    s.hashCode * factor
  }
}

