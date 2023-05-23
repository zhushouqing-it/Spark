package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-03-31-20:34
 */
object Spark02_RDD_File_Par {

    def main(args: Array[String]): Unit = {

        // TODO 准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD
        // textFile 可以将文件作为数据处理的数据源，默认也可以设定分区。
        //         minPartitions:最小分区数量
        //         math.min(defaultParallelism, 2)
        //val rdd: RDD[String] = sc.textFile("datas/1.txt")

        // 如果不想使用默认的分区数量，可以通过第二个参数来指定分区数
        // Spark读取文件，底层其实使用的就是hadoop的读取方式
        // 分区数量的计算方式：
        //      totalSize = 7
        //      goalSize = 7 / 2 = 3 byte
        //      7 / 3 = 2...1 （1.1） 2+1 = 3 个分区
        val rdd: RDD[String] = sc.textFile("datas/1.txt", 2)

        rdd.saveAsTextFile("output")




        // TODO 关闭环境
        sc.stop()
    }

}
