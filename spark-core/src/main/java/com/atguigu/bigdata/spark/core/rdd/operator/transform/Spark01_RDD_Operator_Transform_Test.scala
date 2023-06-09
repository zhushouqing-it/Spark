package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark01_RDD_Operator_Transform_Test {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - map
        val rdd: RDD[String] = sc.textFile("datas/apache.log")

        // 长的字符串转换为短的字符串
        val mapRDD: RDD[String] = rdd.map(
            line => {
                val datas: Array[String] = line.split(" ")
                datas(6)
            }
        )

        mapRDD.collect().foreach(println)


        sc.stop()


    }

}
