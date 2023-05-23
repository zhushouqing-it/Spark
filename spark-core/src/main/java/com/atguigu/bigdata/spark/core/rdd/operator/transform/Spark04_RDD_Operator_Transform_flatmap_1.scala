package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark04_RDD_Operator_Transform_flatmap_1 {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)



        // TODO 算子 - flatmap
        val rdd: RDD[String] = sc.makeRDD(
            List("hello scala", "hello spark")
        )

        val flatRDD: RDD[String] = rdd.flatMap(
            s => {
                s.split(" ")
            }
        )

        flatRDD.collect().foreach(println)


        sc.stop()


    }

}
