package com.atguigu.bigdata.spark.core.rdd.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author ZhuShouqing
 * @create 2023-04-24-11:07
 */
object Spark02_SparkSQL_UDF {

    def main(args: Array[String]): Unit = {

        // TODO : 创建SparkSql的运行环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkUdf")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._

        val df: DataFrame = spark.read.json("datas/user.json")
        df.createOrReplaceTempView("user")

//        spark.sql("select age, concat(\"Name:\",username) as Name_Username from user").show

        // 自定义函数，实现拼接的效果
        spark.udf.register("prefixName", (name: String) => {
            "Name:" + name
        })

        // 用自己自定义的函数
        spark.sql("select age, prefixName(username) as Name_Username from user").show


        // TODO : 关闭环境
        spark.close()


    }


}
