package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-03-31-20:34
 */
object Spark02_RDD_File {

    def main(args: Array[String]): Unit = {

        // TODO 准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD
        // 从文件中创建RDD，将文件中的数据作为处理的数据源
        // path路径默认以当前环境的根路径为准可以写绝对路径，也可以写相对路径
        // path 路径可以是文件的具体路径，也可以是目录的名称
        // val rdd: RDD[String] = sc.textFile("datas/1.txt")
        val rdd: RDD[String] = sc.textFile("datas")

        // path路径还可以使用通配符
        // val rdd: RDD[String] = sc.textFile("datas/1*.txt")

        // path 路径还可以是分布式存储系统 "hdfs://hadoop177:8020/xxx"

        rdd.collect().foreach(println)


        // TODO 关闭环境
        sc.stop()
    }

}
