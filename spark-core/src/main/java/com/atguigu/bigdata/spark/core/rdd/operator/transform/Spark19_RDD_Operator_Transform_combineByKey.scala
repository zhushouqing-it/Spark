package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark19_RDD_Operator_Transform_combineByKey {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - combineByKey
        // 将数据根据不同的规则进行分区内计算和分区间计算
        // 该算子的功能主要是为了 解决reduceByKey只能让分区内和分区间采用同一种规则计算

        val rdd: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1), ("a", 2), ("b", 3),
                ("b", 4), ("b", 5), ("a", 6),
            ), 2
        )


        // combineByKey : 方法需要三个参数
        // 第一个参数表示：将相同key的第一个数据进行结构的转换，实现操作
        // 第二个参数表示：分区内的计算
        // 第三个参数表示：分区间的计算
        val newRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
            v => (v, 1),
            (t: (Int, Int), v) => {
                (t._1 + v, t._2 + 1)
            },
            (t1: (Int, Int), t2: (Int, Int)) => {
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
