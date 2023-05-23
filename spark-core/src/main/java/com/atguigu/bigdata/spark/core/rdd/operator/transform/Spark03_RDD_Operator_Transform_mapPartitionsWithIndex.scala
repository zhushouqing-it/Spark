package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark03_RDD_Operator_Transform_mapPartitionsWithIndex {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - mapPartitions
        val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

        val mpiRDD: RDD[Int] = rdd.mapPartitionsWithIndex(
            (index, iter) => {
                if (index == 1) {
                    iter
                } else {
                    Nil.iterator // Nil 是空的list
                }

            }
        )


        mpiRDD.collect().foreach(println)

        sc.stop()


    }

}
