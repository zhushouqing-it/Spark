package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark14_RDD_Operator_Transform_partitionBy {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - key - value 类型
        val rdd = sc.makeRDD(List(1, 2, 3, 4),2)

        val mapRDD = rdd.map((_, 1))

        // partitionBy 必须基于有kv类型的数据才能调用
        // partitionBy 根据指定的分区规则对数据进行重分区
        mapRDD.partitionBy(new HashPartitioner(2))
                .saveAsTextFile("output")


        sc.stop()


    }

}
