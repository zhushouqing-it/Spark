package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark05_RDD_Operator_Transform_glom_Test {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)



        // TODO 算子 - glom
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

        // 取到每个分区中的最大值，然后相加
        // [1,2],[3,4]
        // [2],[4]
        // [6]

        val glomRDD: RDD[Array[Int]] = rdd.glom()

        val maxRDD: RDD[Int] = glomRDD.map(
            array => array.max
        )

        println(maxRDD.collect().sum)




        sc.stop()


    }

}
