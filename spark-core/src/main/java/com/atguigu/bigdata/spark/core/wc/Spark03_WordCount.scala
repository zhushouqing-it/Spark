package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-03-18-19:43
 */
object Spark03_WordCount {
    def main(args: Array[String]): Unit = {


        // TODO 建立和Spark框架的连接
        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        // TODO 执行业务操作

        val lines: RDD[String] = sc.textFile("datas")

        val words: RDD[String] = lines.flatMap(_.split(" "))

        val wordAndOne: RDD[(String, Int)] = words.map(
            word => (word, 1)
        )

        // TODO Spark 框架提供了更多的功能，可以将分组和聚合使用一个方法实现
        // reduceByKey: 相同的key的数据，可以对value进行reduce聚合
//        wordAndOne.reduceByKey((x, y) => {
//            x + y
//        })
        val wordToCount: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)

        val result: Array[(String, Int)] = wordToCount.collect()

        result.foreach(println)

        // TODO 关闭连接
        sc.stop()
    }
}
