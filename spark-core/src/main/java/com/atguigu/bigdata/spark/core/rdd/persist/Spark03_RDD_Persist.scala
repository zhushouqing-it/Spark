package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-19-22:30
 */
object Spark03_RDD_Persist {

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

        // 将数据放到缓存中进行存储。然后下面的就不要从头开始执行了。
        // cache默认持久化的操作，只能将数据保存到内存中，如果想要保存到磁盘，需要更改存储级别
        //mapRDD.cache()
        // 持久化操作必须再行动算子执行时完成的。
        mapRDD.persist(StorageLevel.DISK_ONLY)

        val result: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

        result.collect().foreach(println)
        println("***************************************")

        val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

        groupRDD.collect().foreach(println)

        sc.stop()


    }

}
