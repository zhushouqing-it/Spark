package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark02_RDD_Operator_Transform_mapPartitions {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - mapPartitions
        //
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

        // mapPartitions: 可以以分区为单位进行数据转换操作
        //                但是会将整个分区的数据加载到内存进行引用
        //                如果处理完的数据是不会被释放掉的，存在对象的引用
        //                在内存较小，数据量较大的场合下，容易出现内存溢出
        val mapRDD: RDD[Int] = rdd.mapPartitions(
            iter => {
                println(">>>>>>>>")
                iter.map(_ * 2)
            }
        )

        mapRDD.collect().foreach(println)

        sc.stop()


    }

}
