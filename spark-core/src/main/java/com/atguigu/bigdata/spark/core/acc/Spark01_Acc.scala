package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-20-21:04
 */
object Spark01_Acc {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")

        val sc = new SparkContext(sparkConf)


        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        // reduce : 分区内的计算和分区间的计算
//        val i: Int = rdd.reduce(_ + _)
//
//        println(i)

        var sum = 0
        rdd.foreach(
            num => {
                sum += num
            }
        )

        println("sum=" + sum)

        sc.stop()


    }
}
