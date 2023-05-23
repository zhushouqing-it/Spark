package com.atguigu.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark07_RDD_Operator_Transform_filter {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - filter
        val rdd = sc.makeRDD(List(1, 2, 3, 4))

        // 保留奇数
        val filterRDD: RDD[Int] = rdd.filter(num => num % 2 == 1)

        filterRDD.collect().foreach(println)

        sc.stop()


    }

}
