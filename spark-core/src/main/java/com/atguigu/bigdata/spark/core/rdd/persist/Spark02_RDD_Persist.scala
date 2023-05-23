package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-19-22:30
 */
object Spark02_RDD_Persist {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)


        val list = List("hello scala","hello spark")

        val rdd: RDD[String] = sc.makeRDD(list)

        val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

        val mapRDD: RDD[(String, Int)] = flatRDD.map{
            println("@@@@@@@@@@@@@@@@@@@@@@@")
            word => (word, 1)
        }

        val result: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

        result.collect().foreach(println)
        println("***************************************")

        // RDD不存储数据，这里mapRDD重用，还是要从头开始执行，并不会省去前面的步骤
        val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

        groupRDD.collect().foreach(println)

        sc.stop()


    }

}
