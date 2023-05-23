package com.atguigu.bigdata.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-19-19:02
 */
object Spark01_RDD_Dep {
    def main(args: Array[String]): Unit = {


        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)


        val lines: RDD[String] = sc.textFile("datas/2.txt")
        println(lines.toDebugString)
        println("***************************")

        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.toDebugString)
        println("***************************")


        val wordAndOne: RDD[(String, Int)] = words.map(
            word => (word, 1)
        )
        println(wordAndOne.toDebugString)
        println("***************************")

        val wordToCount: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
        println(wordToCount.toDebugString)
        println("***************************")

        val result: Array[(String, Int)] = wordToCount.collect()
        result.foreach(println)

        sc.stop()

    }

}
