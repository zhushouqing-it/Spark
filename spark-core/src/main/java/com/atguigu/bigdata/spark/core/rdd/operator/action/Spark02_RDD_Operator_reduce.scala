package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-18-20:17
 */
object Spark02_RDD_Operator_reduce {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4))


        // TODO -行动算子
        // reduce
        //val i: Int = rdd.reduce(_ + _)
        // println(i)

        // collect ：方法会将不同分区的数据按照分区的顺序采集到Driver端内存中，形成数组。
//        val ints: Array[Int] = rdd.collect()
//        println(ints.mkString(","))

        // count : 数据源中数据的个数
        val l: Long = rdd.count()
        println(l)

        // first: 获取数据源数据中的第一个
        val firstRDD: Int = rdd.first()
        println(firstRDD)

        // take : 取数据数据源中n个
        val takeRDD: Array[Int] = rdd.take(3)
        println(takeRDD.mkString(","))

        // takeOrdered : 数据排序后，取N个数据
        val takeOrRDD: Array[Int] = rdd.takeOrdered(3)
        println(takeOrRDD.mkString(","))

        sc.stop()

    }

}
