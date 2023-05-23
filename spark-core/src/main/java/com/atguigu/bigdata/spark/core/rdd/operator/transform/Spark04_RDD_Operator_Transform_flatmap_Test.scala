package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark04_RDD_Operator_Transform_flatmap_Test {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)



        // TODO 算子 - flatmap
        val rdd: RDD[Any] = sc.makeRDD(
            List(List(1, 2), 3, List(4, 5))
        )

        val flatRDD: RDD[Any] = rdd.flatMap(
            data => {
                data match {
                    case list: List[_] => list
                    case dat => List(dat)
                }
            }
        )

        flatRDD.collect().foreach(println)

        sc.stop()


    }

}
