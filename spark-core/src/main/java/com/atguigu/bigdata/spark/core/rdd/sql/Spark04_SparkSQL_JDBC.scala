package com.atguigu.bigdata.spark.core.rdd.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator


/**
 * @author ZhuShouqing
 * @create 2023-04-24-11:07
 */
object Spark04_SparkSQL_JDBC {

    def main(args: Array[String]): Unit = {


        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkUdf")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        // 读取Mysql的数据
        val df = spark.read
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/atguigudb?characterEncoding=utf-8&serverTimezone=UTC")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("user", "root")
                .option("password", "123456")
                .option("dbtable", "countries")
                .option("useSSL","false")
                .load()


        //df.show

        // 保存数据
        df.write
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/atguigudb?characterEncoding=utf-8&serverTimezone=UTC")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("user", "root")
                .option("password", "123456")
                .option("dbtable", "countries1")
                .option("useSSL","false")
                .mode(SaveMode.Append)
                .save()
        spark.close()

    }


}
