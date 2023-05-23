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
object Spark03_Req1_HotCategoryTop10Analysis3 {


    def main(args: Array[String]): Unit = {


        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")

        val sc = new SparkContext(sparkConf)


        val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
        actionRDD.cache()
//        actionRDD.checkpoint()

        // TODO ： 存在的问题
        //         存在大量的shuffle操作（reduceByKey）
        //         reduceByKey 聚合算子，Spark会提供优化，缓存

        //这里我们可以直接转换数据结构将数据转换成如下
        // action ==》
        //            点击场合 ===》 (品类,(1,0,0))
        //            下单场合 ===》 (品类,(0,1,0))
        //            支付场合 ===》 (品类,(0,0,1))

        // 得到最后的结果后在进行两两的聚合操作
        // 因为下单，支付场景下会有多个id，我们会将一行数据炸裂为多行数据，所以用flatmap
        val sourceRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
            action => {
                val actionArrRDD: Array[String] = action.split("_")
                if (actionArrRDD(6) != "-1") {
                    // 点击场合
                    // 因为这里用了flatmap，要求返回结果为List
                    List((actionArrRDD(6), (1, 0, 0)))
                } else if (actionArrRDD(8) != "null") {
                    // 下单场合
                    val ids: Array[String] = actionArrRDD(8).split(",")
                    ids.map(
                        id => (id, (0, 1, 0))
                    )
                } else if (actionArrRDD(10) != "null") {
                    // 支付场合
                    val ids: Array[String] = actionArrRDD(10).split(",")
                    ids.map(
                        id => (id, (0, 0, 1))
                    )
                } else {
                    Nil
                }
            }
        )


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
