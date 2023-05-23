package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-19-22:30
 */
object Spark04_RDD_Persist {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)
        sc.setCheckpointDir("cp")

        val list = List("hello scala","hello spark")

        val rdd: RDD[String] = sc.makeRDD(list)

        val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

        val mapRDD: RDD[(String, Int)] = flatRDD.map{
            println("@@@@@@@@@@@@@@@@@@@@@@@")
            word => (word, 1)
        }

        // checkpoint 需要落盘，需要指定检查点保存路径
        // 检查点路径中保存的文件，当作业执行完毕后，不会被删除
        // 一般保存路径都是分布式文件系统，例如hdfs
        mapRDD.checkpoint()

        val result: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

        result.collect().foreach(println)
        println("***************************************")

        val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

        groupRDD.collect().foreach(println)

        sc.stop()


    }

}
