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
object Spark01_Req1_HotCategoryTop10Analysis {


    def main(args: Array[String]): Unit = {

        // TODO : TOP10 热门品类
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")

        val sc = new SparkContext(sparkConf)


        // 1. 读取原始数据
        val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

        // 2. 统计出每个品类的点击数  action => (品类ID,Count)
        // 如果点击的品类 ID 和产品 ID 为-1，表示数据不是点击数据
        // 所以要先将我们的点击数据给过滤出来，然后在进行统计
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


        // 3. 统计出每个品类的下单数  action => (品类ID,Count)
        // 针对于下单行为，一次可以下单多个商品，所以品类 ID 和产品 ID 可以是多个
        // ，id 之间采用逗号分隔，如果本次不是下单行为，则数据采用 null 表示
        // 根据上述，先将下单数据过滤出来
        val orderRDD: RDD[String] = actionRDD.filter(
            action => {
                val actionArrRDD: Array[String] = action.split("_")
                actionArrRDD(8) != "null"
            }
        )

        // 因为品类ID和产品ID会有多个
        // 1，2，3 => [1,2,3] ，我们最后想要的是，将一个整体拆分为个体
        // 进来一条变为三条，这个属于flatmap，然后再经过map，最后reduceByKey
        // ====> [(1,count),(2,count),(3,count)]
        // 先炸裂，再Map，最后reduce
        val orderCountRDD: RDD[(String, Int)] = orderRDD.flatMap(
            action => {
                val actionArrRDD: Array[String] = action.split("_")
                val orderCategoryIds: Array[String] = actionArrRDD(8).split(",")
                orderCategoryIds
            }
        ).map(
            id => (id, 1)
        ).reduceByKey(_ + _)
        //
//        val orderCount: RDD[(String, Int)] = orderRDD.flatMap(
//            action => {
//                val actionArrRDD: Array[String] = action.split("_")
//                val orderCategoryIds: Array[String] = actionArrRDD(8).split(",")
//                orderCategoryIds.map( id => (id,1))
//            }
//        ).reduceByKey(_+_)
// 4. 统计出每个品类的支付数  action => (品类ID,Count)
// 支付行为与下单行为类似
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

        // 5. 进行排序，统计出Top10
        // (品类,点击count)，(品类,下单count)，(品类,支付count)
        // ====> (品类,(点击count,下单count,支付count))
        // 最后先按照点击数，如果点击数相同再按照下单数，最后按照支付数
        // 元组排序：先比较第一个，然后比较第二个，然后比较第三个，以此类推
        // 将不同的数据源，连接在一块，有 join、zip、leftOuterJoin、coGroup
        // 最后分析：只能使用cogroup = connect + group
        // join、leftOuterJoin ，无法保证获取到全部的key，zip （拉链）跟分区、分区数有关，无法和key联系
        val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
        clickCountRDD.cogroup(orderCountRDD, payCountRDD)

        val analysisRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
            case (clickIter, orderIter, payIter) => {
                var clickCnt = 0
                // 如果没有值就给0，如果有值，就把值赋给它，这个迭代器里面只有一个值，因为前面的品类经过reduce只可能是一个
                val iterator1: Iterator[Int] = clickIter.iterator
                if (iterator1.hasNext) {
                    clickCnt = iterator1.next()
                }

                var orderCnt = 0
                val iterator2: Iterator[Int] = orderIter.iterator
                if (iterator2.hasNext) {
                    orderCnt = iterator2.next()
                }

                var payCnt = 0
                val iterator3: Iterator[Int] = payIter.iterator
                if (iterator3.hasNext) {
                    payCnt = iterator3.next()
                }

                (clickCnt, orderCnt, payCnt)
            }
        }

        // 进行排序，取前10
        val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)

        // 6. 将结果打印到控制台
        resultRDD.foreach(println)

        sc.stop()
    }

}
