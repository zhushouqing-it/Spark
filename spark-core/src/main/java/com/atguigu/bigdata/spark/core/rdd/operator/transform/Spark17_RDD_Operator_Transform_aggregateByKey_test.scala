package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark17_RDD_Operator_Transform_aggregateByKey_test {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - aggregateByKey
        // 将数据根据不同的规则进行分区内计算和分区间计算
        // 该算子的功能主要是为了 解决reduceByKey只能让分区内和分区间采用同一种规则计算

        val rdd: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1), ("a", 2), ("b", 3),
                ("b", 4), ("b", 5), ("a", 6),
            ), 2
        )


        // aggregateByKey 最终的返回数据结果应该和初始值的类型保持一致
//        val newRDD: RDD[(String, String)] = rdd.aggregateByKey("")(_ + _, _ + _)
//
//        newRDD.collect().foreach(println)

        // 获取相同key的数据的平均值 => (a,3) (b,4)
        val newRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
            (t, v) => {
                (t._1 + v, t._2 + 1)
            },
            (t1, t2) => {
                (t1._1 + t2._1, t1._2 + t2._2)
            }
        )

        val result: RDD[(String, Int)] = newRDD.mapValues {
            case (num, cnt) => {
                num / cnt
            }
        }

        result.collect().foreach(println)


        sc.stop()


    }

}
