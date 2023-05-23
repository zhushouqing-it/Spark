package com.atguigu.bigdata.spark.core.exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-21-22:58
 */

// TODO : Top10热门品类中每个品类的TOP10活跃Session统计


object Spark05_Req2_HotCategoryTop10SessionAnalysis {


    def main(args: Array[String]): Unit = {


        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")

        val sc = new SparkContext(sparkConf)


        val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
        actionRDD.cache()


        val top10Ids: Array[String] = top10Category(actionRDD)

        // 1. 过滤原始数据，保留点击和前10品类ID
        val filterActionRDD: RDD[String] = actionRDD.filter(
            action => {
                val datas = action.split("_")
                if (datas(6) != "-1") {
                    val bool: Boolean = top10Ids.contains(datas(6))
                    bool
                } else {
                    false
                }
            }
        )

        // 2. action ====》 ((品类ID，sessionId),count)
        // 然后在进行聚合
        val reduceRDD: RDD[((String, String), Int)] = filterActionRDD.map(
            action => {
                val datas: Array[String] = action.split("_")
                ((datas(6), datas(2)), 1)
            }
        ).reduceByKey(_ + _)


        // 3. 最后要求的结果是 (品类ID，(sessionId,count))再进行排序
        // ((品类ID，sessionId),count)  ====》 (品类ID，(sessionId,count))
        val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
            case ((cId, sId), sum) => {
                (cId, (sId, sum))
            }

        }

        // 4. 按照品类ID进行分组,因为要分组求top10
        val groupByRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()


        // 5. 降序排序，求前10
        val resultRDD: RDD[(String, List[(String, Int)])] = groupByRDD.mapValues(
            iter => iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
        )

        resultRDD.collect().foreach(println)

        sc.stop()
    }


    def top10Category(actionRDD: RDD[String]) = {
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

        val resultIds: Array[String] = analysisRDD.sortBy(_._2, false).take(10).map(_._1)
        resultIds
    }

}
