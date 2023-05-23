package com.atguigu.bigdata.spark.core.exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-21-22:58
 */

// TODO : TOP10问题
//       鞋：点击数、下单数、支付数
//       品类：点击数、下单数、支付数
//       最后先按照点击数，如果点击数相同再按照下单数，最后按照支付数
object Spark02_Req1_HotCategoryTop10Analysis2 {


    def main(args: Array[String]): Unit = {


        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")

        val sc = new SparkContext(sparkConf)


        val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
        actionRDD.cache()
//        actionRDD.checkpoint()

        // TODO ： 存在的问题
        //         actionRDD被重复使用多次，优化：可以将其放在缓存之中  actionRDD.cache()
        //         cogroup 可能存在shuffle，性能较低,优化：
        //          (品类,点击count)，  ===> (品类，(点击count，0,0))
        //          (品类,下单count)，  ===> (品类，(0，下单count,0))
        //          (品类,支付count)   ===> (品类，(0，0,支付count))
        //          最后再两两进行聚合 得到 ====> (品类,(点击count,下单count,支付count))

        val clickRDD: RDD[String] = actionRDD.filter(
            action => {
                //  注意这里返回的是字符串数组
                val actionArrRDD: Array[String] = action.split("_")
                actionArrRDD(6) != "-1"
            }
        )

        val clickCountRDD: RDD[(String, Int)] = clickRDD.map(
            action => {
                val actionArrRDD: Array[String] = action.split("_")
                (actionArrRDD(6), 1)
            }
        ).reduceByKey(_ + _)


        val orderRDD: RDD[String] = actionRDD.filter(
            action => {
                val actionArrRDD: Array[String] = action.split("_")
                actionArrRDD(8) != "null"
            }
        )


        val orderCountRDD: RDD[(String, Int)] = orderRDD.flatMap(
            action => {
                val actionArrRDD: Array[String] = action.split("_")
                val orderCategoryIds: Array[String] = actionArrRDD(8).split(",")
                orderCategoryIds
            }
        ).map(
            id => (id, 1)
        ).reduceByKey(_ + _)


        val payRDD: RDD[String] = actionRDD.filter(
            action => {
                val actionArrRDD: Array[String] = action.split("_")
                actionArrRDD(10) != "null"
            }
        )

        val payCountRDD: RDD[(String, Int)] = payRDD.flatMap(
            action => {
                val actionArrRDD: Array[String] = action.split("_")
                val payCategoryIds: Array[String] = actionArrRDD(10).split(",")
                payCategoryIds
            }
        ).map(
            id => (id, 1)
        ).reduceByKey(_ + _)

        val rdd1: RDD[(String, (Int, Int, Int))] = clickCountRDD.map {
            case (cid, count) => (cid, (count, 0, 0))
        }

        val rdd2: RDD[(String, (Int, Int, Int))] = orderCountRDD.map {
            case (cid, count) => (cid, (0, count, 0))
        }

        val rdd3: RDD[(String, (Int, Int, Int))] = payCountRDD.map {
            case (cid, count) => (cid, (0, 0, count))
        }

        // 将三个数据源合并在一起，统一进行聚合计算
        val sourceRDD: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)

        val analysisRDD: RDD[(String, (Int, Int, Int))] = sourceRDD.reduceByKey(
            (t1, t2) => {
                (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
            }
        )

        val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)


        resultRDD.foreach(println)

        sc.stop()
    }

}
