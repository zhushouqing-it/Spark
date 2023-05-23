package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark12_RDD_Operator_Transform_sortBy {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - sortBy
        // 分区内有序
        val rdd: RDD[Int] = sc.makeRDD(List(1, 3, 5, 4, 2,6), 2)

        val newRDD: RDD[Int] = rdd.sortBy(
            num => num
        )

        // newRDD.collect().foreach(println)

        newRDD.saveAsTextFile("output")

        sc.stop()


    }

}
