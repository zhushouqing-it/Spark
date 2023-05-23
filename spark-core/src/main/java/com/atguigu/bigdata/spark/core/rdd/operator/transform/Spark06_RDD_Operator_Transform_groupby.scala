package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark06_RDD_Operator_Transform_groupby {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - groupBy
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

        // groupBy会将数据源中的每一个数据进行分组判断，根据返回的分组Key进行分组
        // 相同的Key值的数据会放置在一个组中
//        val groupByRDD: RDD[(String, Iterable[Int])] = rdd.groupBy(
//            num => {
//                if (num % 2 == 1) {
//                    "奇数"
//                } else {
//                    "偶数"
//                }
//            }
//        )
        val groupByRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(
            num => num % 2
        )

        groupByRDD.collect().foreach(println)

        sc.stop()


    }

}
