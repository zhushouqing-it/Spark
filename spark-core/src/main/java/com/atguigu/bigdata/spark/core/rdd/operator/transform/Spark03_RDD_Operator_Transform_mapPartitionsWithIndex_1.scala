package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark03_RDD_Operator_Transform_mapPartitionsWithIndex_1 {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - mapPartitions
        val rdd = sc.makeRDD(List(1, 2, 3, 4))

        val mpiRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
            (index, iter) => {
                iter.map(
                    num => (index, num)
                )
            }
        )

        mpiRDD.collect().foreach(println)

        sc.stop()


    }

}
