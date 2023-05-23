package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-18-20:17
 */
object Spark03_RDD_Operator_aggregate {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4))


        // TODO -行动算子
        // aggregateByKey: 初始值只会参与分区内计算
        // aggregate : 初始值会参与分区内计算并且会参与分区间计算
        //val result: Int = rdd.aggregate(0)(_ + _, _ + _)

        val result: Int = rdd.fold(0)(_ + _)

        println(result)

        sc.stop()

    }

}
