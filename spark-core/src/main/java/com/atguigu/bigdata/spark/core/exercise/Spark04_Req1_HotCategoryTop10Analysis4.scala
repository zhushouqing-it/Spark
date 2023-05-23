package com.atguigu.bigdata.spark.core.exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

/**
 * @author ZhuShouqing
 * @create 2023-04-21-22:58
 */

// TODO : TOP10问题
//       鞋：点击数、下单数、支付数
//       品类：点击数、下单数、支付数
//       最后先按照点击数，如果点击数相同再按照下单数，最后按照支付数
object Spark04_Req1_HotCategoryTop10Analysis4 {


    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")

        val sc = new SparkContext(sparkConf)


        val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
        actionRDD.cache()
//        actionRDD.checkpoint()

        val acc = new HotCategoryAccumulator()
        sc.register(acc, "hotCategory")

        actionRDD.foreach(
            action => {
                val actionArrRDD: Array[String] = action.split("_")
                if (actionArrRDD(6) != "-1") {
                    acc.add(((actionArrRDD(6), "click")))
                } else if (actionArrRDD(8) != "null") {
                    // 下单场合
                    val ids: Array[String] = actionArrRDD(8).split(",")
                    ids.foreach(
                        id => acc.add((id, "order"))
                    )
                } else if (actionArrRDD(10) != "null") {
                    // 支付场合
                    val ids: Array[String] = actionArrRDD(10).split(",")
                    ids.foreach(
                        id => acc.add((id, "pay"))
                    )
                }
            }
        )

        val accValue: mutable.Map[String, HotCategory] = acc.value

        val categories: Iterable[HotCategory] = accValue.map(_._2)

        val sort: List[HotCategory] = categories.toList.sortWith(
            (left, right) => {
                if (left.clickCnt > right.clickCnt) {
                    true
                } else if (left.clickCnt == right.clickCnt) {
                    if (left.orderCnt > right.orderCnt) {
                        true
                    } else if (left.orderCnt == right.orderCnt) {
                        left.payCnt > right.payCnt
                    }
                    else {
                        false
                    }
                }
                else {
                    false
                }
            }
        )

        sort.take(10).foreach(println)

        sc.stop()
    }

    case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

    /**
     * 自定义累加器
     *  1. 继承AccumulatorV2 定义泛型
     *     IN :  (品类ID，行为类型)
     *     OUT : mutable.Map[String,HotCategory]
     *
     * 2. 重写方法
     */
    class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {
        private val hcMap = mutable.Map[String, HotCategory]()

        override def isZero: Boolean = {
            hcMap.isEmpty
        }

        override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
            new HotCategoryAccumulator()
        }

        override def reset(): Unit = {
            hcMap.clear()
        }

        override def add(v: (String, String)): Unit = {
            val cid = v._1
            val actionType = v._2
            val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
            if (actionType == "click") {
                category.clickCnt += 1
            } else if (actionType == "order") {
                category.orderCnt += 1
            } else if (actionType == "pay") {
                category.payCnt += 1
            }
            hcMap.update(cid, category)

        }

        override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
            val map1 = this.hcMap
            val map2 = other.value

            map2.foreach {
                case (cid, hc) => {
                    val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
                    category.clickCnt += hc.clickCnt
                    category.orderCnt += hc.orderCnt
                    category.payCnt += hc.payCnt
                    map1.update(cid, category)
                }
            }
        }

        override def value: mutable.Map[String, HotCategory] = hcMap
    }

}
