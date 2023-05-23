package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark07_RDD_Operator_Transform_filter_test {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - filter
        // 获取2015年5月17日的请求路径
        val rdd: RDD[String] = sc.textFile("datas/apache.log")

        rdd.filter(
            line => line.contains("17/05/2015")
        ).map(
            line => {
                val datas: Array[String] = line.split(" ")
                datas(6)
            }
        ).collect().foreach(println)

        sc.stop()


    }

}
