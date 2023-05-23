package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark16_RDD_Operator_Transform_groupByKey {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - groupByKey
        val rdd = sc.makeRDD(
            List(("a", 1), ("a", 2), ("a", 3), ("d", 4))
        )


        // groupByKey：将数据源中的数据，相同的key的数据分在一个组中，形成一个对偶元组
        //              元组中的第一个元素就是key
        //              元组中的第二个元素就是相同key的value的集合
        val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()

        groupRDD.collect().foreach(println)

        val groupRDD2: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(
            _._1
        )

        sc.stop()


    }

}
