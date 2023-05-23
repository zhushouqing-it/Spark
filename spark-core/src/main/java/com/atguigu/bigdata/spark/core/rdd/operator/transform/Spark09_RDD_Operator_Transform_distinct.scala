package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark09_RDD_Operator_Transform_distinct {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - distinct
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))

        val rdd1: RDD[Int] = rdd.distinct()

        rdd1.collect().foreach(println)




        sc.stop()


    }

}
