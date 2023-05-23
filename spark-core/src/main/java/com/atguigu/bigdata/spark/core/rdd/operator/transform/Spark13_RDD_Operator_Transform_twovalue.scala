package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark13_RDD_Operator_Transform_twovalue {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - 双value类型

        val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
        val rdd2 = sc.makeRDD(List(3, 4, 5, 6))

        // 交集 并集 差集 需要两个数据的类型相同


        // 交集  intersection
        val dataRDD1 = rdd1.intersection(rdd2)
        println(dataRDD1.collect().mkString(","))

        // 并集 union
        val dataRDD2: RDD[Int] = rdd1.union(rdd2)
        println(dataRDD2.collect().mkString(","))

        // 差集 subtract
        val dataRDD3: RDD[Int] = rdd1.subtract(rdd2)
        println(dataRDD3.collect().mkString(","))

        // 拉链 zip
        // 将两个RDD中的元素，以键值对的形式进行合并，其中键值对中的Key为第一个RDD
        // 中的元素，value为第二个RDD中的相同位置的元素。

        // 拉链的两个数据源的类型可以不同，但是分区数必须相同且每个分区中的元素数量相同
        val dataRDD4: RDD[(Int, Int)] = rdd1.zip(rdd2)
        println(dataRDD4.collect().mkString(","))


        sc.stop()


    }

}
