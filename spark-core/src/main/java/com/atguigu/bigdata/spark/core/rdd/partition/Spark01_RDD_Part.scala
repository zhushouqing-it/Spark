package com.atguigu.bigdata.spark.core.rdd.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-20-20:42
 */
object Spark01_RDD_Part {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Part")

        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(
            ("nba", "xxxxxxxx"),
            ("cba", "xxxxxxxx"),
            ("wnba", "xxxxxxxx"),
            ("nba", "xxxxxxxx")
        ))
        val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)

        partRDD.saveAsTextFile("output")

        sc.stop()

    }

    /**
     * 自定义分区器
     * 1. 基础partitioner
     * 2. 重写方法
     */
    class MyPartitioner extends Partitioner {
        // 分区的数量
        override def numPartitions: Int = 3

        // 根据数据的key值返回数据所在的分区索引（从0开始）
        override def getPartition(key: Any): Int = {

            key match {
                case "nba" => 0
                case "wnba" => 1
                case _ => 2
            }
//            if (key == "nba") {
//                0
//            } else if (key == "wnba") {
//                1
//            } else if (key == "cba") {
//                2
//            } else {
//                2
//            }
        }
    }

}
