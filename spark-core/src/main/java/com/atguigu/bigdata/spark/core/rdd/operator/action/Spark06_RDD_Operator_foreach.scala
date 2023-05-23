package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-18-20:17
 */
object Spark06_RDD_Operator_foreach {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        // foreach 其实是Driver端内存集合的循环遍历
        rdd.collect().foreach(println)
        println("************************")
        // foreach 其实是Executor端内存数据打印,分布式打印
        rdd.foreach(println)

        // 算子：Operator（操作）
        //      RDD的方法和scala集合对象的方法不一样
        //      集合对象的方法都是再同一个节点的内存中完成的
        //      RDD的方法可以将计算逻辑发送到Executor端（分布式节点）执行
        //      为了区分不同的处理效果，所以将RDD的方法称之为算子。
        //       RDD的方法外部的操作都是在Driver端执行的，而方法内部的逻辑代码是在Executor端执行的

        sc.stop()

    }

}
