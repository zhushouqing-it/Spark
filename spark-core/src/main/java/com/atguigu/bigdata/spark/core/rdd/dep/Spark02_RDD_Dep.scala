package com.atguigu.bigdata.spark.core.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author ZhuShouqing
 * @create 2023-04-19-19:02
 */
object Spark02_RDD_Dep {
    def main(args: Array[String]): Unit = {


        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)


        val lines: RDD[String] = sc.textFile("datas/2.txt")
        println(lines.dependencies)
        println("***************************")

        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.dependencies)
        println("***************************")


        val wordAndOne: RDD[(String, Int)] = words.map(
            word => (word, 1)
        )
        println(wordAndOne.dependencies)
        println("***************************")

        val wordToCount: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
        println(wordToCount.dependencies)
        println("***************************")

        val result: Array[(String, Int)] = wordToCount.collect()
        result.foreach(println)

        sc.stop()

    }

}
