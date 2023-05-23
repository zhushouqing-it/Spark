//package com.atguigu.bigdata.spark.streaming
//
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import scala.collection.parallel.mutable
//
///**
// * @author ZhuShouqing
// * @create 2023-05-14-13:25
// */
//object SparkStreaming02_Queue {
//
//    def main(args: Array[String]): Unit = {
//
//
//        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
//
//        val ssc = new StreamingContext(sparkConf, Seconds(3))
//
//        // 创建RDD队列
//        val rddQueue = new mutable.Queue[RDD[Int]]()
//
//        val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)
//
//        val mappedStream = inputStream.map((_, 1))
//
//        val reducedStream = mappedStream.reduceByKey(_ + _)
//
//        reducedStream.print()
//
//        ssc.start()
//
//        for (i <- 1 to 5) {
//            rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
//            Thread.sleep(2000)
//        }
//
//        ssc.awaitTermination()
//
//    }
//
//}
