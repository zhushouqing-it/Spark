package com.atguigu.bigdata.spark.core.rdd.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._


/**
 * @author ZhuShouqing
 * @create 2023-04-24-11:07
 */
object Spark05_SparkSQL_HIVE {

    def main(args: Array[String]): Unit = {


        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkUdf")
        val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

        // 使用SparkSQL连接外置的HIVE
        // 1. 拷贝 Hive-site.xml 文件到classpath
        // 2. 启用hive的支持
        // 3. 增加对应的依赖关系(包含mysql驱动)

        spark.sql("show tables").show



    }


}
