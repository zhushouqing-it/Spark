package com.atguigu.bigdata.spark.core.rdd.io

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-20-20:53
 */
object Spark01_RDD_IO_save {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("IO")

        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(
            ("a",1),
            ("b",2),
            ("c",3)        ))

        rdd.saveAsTextFile("output1")
        rdd.saveAsObjectFile("output2")
        rdd.saveAsSequenceFile("output3")

        sc.stop()


    }

}
