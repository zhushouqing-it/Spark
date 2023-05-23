package com.atguigu.bigdata.spark.core.rdd.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._


/**
 * @author ZhuShouqing
 * @create 2023-04-24-11:07
 */
object Spark03_SparkSQL_UDAF2 {

    def main(args: Array[String]): Unit = {


        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkUdf")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        val df: DataFrame = spark.read.json("datas/user.json")

        import spark.implicits._

        // 早期版本中，Spark不能再sql中使用强类型的UDAF操作
        // SQL & DSL
        // 早期的UDAF强类型聚合函数使用DSL语法操作
        val ds: Dataset[User] = df.as[User]

        // 将UDAF函数转换为查询的列对象
        val udafCol: TypedColumn[User, Long] = new MyAvgUDAF().toColumn

        ds.select(udafCol).show()

        // TODO : 关闭环境
        spark.close()


    }


    case class User(username: String,age: Long)

    // scala中样例类默认参数的属性是不能够改的，需要用var指明
    case class Buff(var total: Long, var count: Long)

        /*
    自定义聚合函数类，计算年龄的平均值
     1. 继承  Aggregator
     IN: 输入的数据类型
     BUF: 缓冲区数据类型
     OUT: 输出数据类型
     2. 重写方法
     */
    class MyAvgUDAF extends Aggregator[User, Buff, Long] {

        // z & zero ：初始值或者零值
        // 缓冲区初始化
        override def zero: Buff = {
            Buff(0L, 0L)
        }

        // 根据输入的数据来更新缓冲区的数据
        override def reduce(buff: Buff, in: User): Buff = {
            buff.total = buff.total + in.age
            buff.count = buff.count + 1
            buff
        }

        // 合并缓冲区
        override def merge(buff1: Buff, buff2: Buff): Buff = {
            buff1.total = buff1.total + buff2.total
            buff1.count = buff1.count + buff2.count
            buff1
        }

        // 计算结果
        override def finish(buff: Buff): Long = {

            buff.total / buff.count

        }

        // 缓冲区的编码操作,固定写法，自定义类 Encoders.product
        override def bufferEncoder: Encoder[Buff] = Encoders.product


        // 输出的编码操作 固定写法，Scala已经存在的类就 Encoders.scala类型
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }


}
