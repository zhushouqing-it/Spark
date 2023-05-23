package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-18-20:17
 */
object Spark04_RDD_Operator_countByKey {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4))

        val rdd2: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1), ("a", 2), ("a", 3)
            )
        )


        // TODO -行动算子
        val intToLong: collection.Map[Int, Long] = rdd.countByValue()
        val stringToLong: collection.Map[String, Long] = rdd2.countByKey()

        println(stringToLong)

        sc.stop()

    }

}
