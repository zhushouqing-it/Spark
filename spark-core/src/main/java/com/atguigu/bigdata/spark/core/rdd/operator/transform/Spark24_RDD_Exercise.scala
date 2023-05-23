package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark24_RDD_Exercise {

    def main(args: Array[String]): Unit = {

        // TODO 案例实操
        // 统计每个省份每个广告点击次数的前三名。

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)

        // 1. 获取原始数据：时间戳 省份 城市 用户 广告
        val dataRDD: RDD[String] = sc.textFile("D:\\java\\project\\bigdataproject\\Spark\\datas\\agent.log")

        // 2. 对原始数据进行转换，由于我们需要统计每个省份每个广告的点击次数
        // 所以我们需要进行map line => ((省份,广告),1)
        val mapRDD: RDD[((String, String), Int)] = dataRDD.map(
            line => {
                val strings: Array[String] = line.split(" ")
                val tuple: ((String, String), Int) = ((strings(1), strings(4)), 1)
                tuple
            }
        )

        // 3. 对得到的数据按照分组进行聚合,分组之间两两聚合，所以很容易想到我们的性能很高的reduceByKey
        val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)

        // 4. 通过第三步我们得到的数据就是 ((省份,广告),sum)，我们最终想要的结果是每个省份，后面跟上我们的(广告,sum)
        //    所以需要再一次的进行转换。
        val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
            // 这里用一个模式匹配
            case ((province, adv), sum) => {
                (province, (adv, sum))
            }
        }

        // 5. 我们最后需要的是分组topN的问题，
        // 所以我们要将相同key的值分为一组
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()

        // 6. 分完组之后我们需要将，分组之类的数据进行排序，然后倒序排序，取出前三
        //    由于我们的key不变，只需要对value进行操作，所以可以使用算子mapValues
        val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
            line => {
                line.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
            }
        )

        resultRDD.collect().foreach(println)

        sc.stop()


    }

}
