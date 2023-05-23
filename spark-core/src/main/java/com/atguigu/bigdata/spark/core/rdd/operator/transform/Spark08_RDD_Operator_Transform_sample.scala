package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark08_RDD_Operator_Transform_sample {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - sample
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

        // sample 算子需要传递三个参数
        // 1. 第一个参数表示，抽取数据后是否将数据返回 true（放回），false（丢弃）
        // 2. 第二个参数表示，
        //      如果抽取不放回的场合，数据源中每条数据可能被抽取的概率 基准值的概念。
        //      如果是抽取放回的场合，表示数据源中的每条数据被抽取的可能次数。
        // 3. 第三个参数表示，抽取数据时随机算法的种子
        //                  如果不传递第三个参数，那么使用的是当前的系统时间
//        println(rdd.sample(
//            false,
//            0.4,
//            1 // 为1表示每次执行的结果都是一样的
//        ).collect().mkString(","))

        println(rdd.sample(
            true,
            2
           // 1 // 为1表示每次执行的结果都是一样的
        ).collect().mkString(","))

        sc.stop()


    }

}
