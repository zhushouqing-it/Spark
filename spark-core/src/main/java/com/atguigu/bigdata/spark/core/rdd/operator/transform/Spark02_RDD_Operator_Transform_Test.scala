package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark02_RDD_Operator_Transform_Test {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - mapPartitions
        val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

        // 传进一个迭代器，返回一个迭代器
        val mapRDD: RDD[Int] = rdd.mapPartitions(
            iter => {
                List(iter.max).iterator
            }

        )

        mapRDD.collect().foreach(println)

        sc.stop()


    }

}
