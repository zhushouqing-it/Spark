package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-03-31-20:34
 */
object Spark01_RDD_Memory_Par {

    def main(args: Array[String]): Unit = {

        // TODO 准备环境
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        sparkConf.set("spark.default.parallelism","5")

        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD -- 计算功能的封装
        // rdd的并行度 & 分区
        // makeRDD 方法可以传递第二个参数，这个参数表示分区的数量
        // 第二个参数可以不传递，那么makeRDD方法会使用默认值 : defaultParallelism
        //      scheduler.conf.getInt("spark.default.parallelism", totalCores)
        //      spark默认情况下，从配置对象中获取配置参数：spark.default.parallelism
        //      如果获取不到，那么使用totalCores属性，这个属性取值为当前环境的最大可用核数
//        val rdd: RDD[Int] = sc.makeRDD(
//            List(1, 2, 3, 4), 2
//        )

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        // 将处理的数据保存成分区文件
        rdd.saveAsTextFile("output")

        // TODO 关闭环境
        sc.stop()
    }

}
