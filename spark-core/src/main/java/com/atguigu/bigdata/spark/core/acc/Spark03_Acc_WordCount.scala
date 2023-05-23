package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.parallel.mutable
import scala.collection.parallel.mutable.ParMap

/**
 * @author ZhuShouqing
 * @create 2023-04-20-21:04
 */
object Spark03_Acc_WordCount {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")

        val sc = new SparkContext(sparkConf)


        val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "hello", "world"))

        // 累加器：WordCount
        // 创建累加器
        val wcAcc = new MyAccumulator()

        // 向Spark进行注册
        sc.register(wcAcc, "wordCountAcc")

        rdd.foreach(
            word => {
                wcAcc.add(word)
            }
        )


        // 获取累加器的结果
        println(wcAcc.value)

        sc.stop()


    }

    /**
     * 自定义数据累加器: WordCount
     *
     * 1. 继承Accumulator2: 定义泛型
     * IN: 累加器输入的数据类型   String
     * OUT: 累加器返回的数据类型  mutable.ParMap[String,Long]
     *
     * 2. 重写方法
     */
    class MyAccumulator extends AccumulatorV2[String, mutable.ParMap[String, Long]] {

        private var wcMap = mutable.ParMap[String, Long]()

        override def isZero: Boolean = {
            wcMap.isEmpty
        }

        override def copy(): AccumulatorV2[String, ParMap[String, Long]] = {
            new MyAccumulator()
        }

        override def reset(): Unit = {
            wcMap.clear()
        }

        // 获取累加器需要计算的值
        override def add(word: String): Unit = {
            val newCnt: Long = wcMap.getOrElse(word, 0L) + 1
            wcMap.updated(word, newCnt)
        }

        // Driver合并多个累加器
        override def merge(other: AccumulatorV2[String, ParMap[String, Long]]): Unit = {

            val map1 = this.wcMap
            val map2 = other.value
            map2.foreach {
                case (word, count) => {
                    val newCount: Long = map1.getOrElse(word, 0L) + count
                    map1.updated(word, newCount)
                }

            }
        }

        // 累加器结果
        override def value: ParMap[String, Long] = {
            wcMap
        }
    }

}
