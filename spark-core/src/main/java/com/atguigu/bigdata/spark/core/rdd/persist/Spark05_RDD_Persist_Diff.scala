package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-19-22:30
 */
object Spark05_RDD_Persist_Diff {

    def main(args: Array[String]): Unit = {

        // TODO :
        //     cache: 将数据临时存储到内存中进行数据重用
        //            会再血缘关系中添加新的依赖，一旦出现问题，可以重头读取数据
        //     persist: 将数据临时存储再磁盘文件中进行数据重用
        //               涉及到磁盘IO，性能较低，单数数据安全
        //                如果作业执行完毕，临时保存的数据文件就会丢失
        //     checkpoint : 将数据长久的保存再磁盘文件中进行数据重用
        //                  涉及到磁盘IO，性能较低，单数数据安全
        //                  为了保证数据安全，所以一般情况下，会独立执行作业
        //                  为了能够提高效率，一般情况下，是需要和cache联合使用
        //                   执行过程中，会切断血缘关系，重新建立新的血缘关系
        //                    checkpoint等同于改变数据源

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


        mapRDD.cache()
        mapRDD.checkpoint()

        val result: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

        result.collect().foreach(println)
        println("***************************************")

        val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

        groupRDD.collect().foreach(println)

        sc.stop()


    }

}
