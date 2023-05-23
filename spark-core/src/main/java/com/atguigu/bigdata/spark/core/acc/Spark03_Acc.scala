package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-20-21:04
 */
object Spark03_Acc {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")

        val sc = new SparkContext(sparkConf)


        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        // 获取系统的累加器
        // Spark默认就提供了简单数据聚合的累加器
        val sumAcc: LongAccumulator = sc.longAccumulator("sum")

        val mapRDD: RDD[Int] = rdd.map(
            num => {
                // 使用累加器
                sumAcc.add(num)
                num
            }
        )

        // 获取累加器的值
        // 少加: 转换算子map中调用累加器，如果没有行动算子的话，那么它不会执行
        // 多加:
       //  一般情况下，累加器放在行动算子中操作
        mapRDD.collect()
        mapRDD.collect()
        println(sumAcc.value)


        sc.stop()


    }
}
