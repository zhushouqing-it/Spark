package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark06_RDD_Operator_Transform_groupby_1 {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - groupBy
        val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "scala", "hadoop"), 2)


        // 分组和分区没有必然的联系
        val groupByRDD: RDD[(String, Iterable[String])] = rdd.groupBy(
            s => s.substring(0, 1)
        )

        groupByRDD.collect().foreach(println)

        sc.stop()


    }

}
