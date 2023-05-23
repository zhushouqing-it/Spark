package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark15_RDD_Operator_Transform_reduceByKey {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - reduceByKey
        val rdd = sc.makeRDD(
            List(("a", 1), ("a", 2), ("a", 3), ("d", 4))
        )

        // reduceByKey : 按照相同的Key的数据进行value的聚合操作
        // scala 语言中一般的聚合操作都是两两聚合，spark基于scala开发的，所以他的聚合也是两两聚合
        //【1，2，3】
        // reduceByKey中如果key的数据只有一个，是不会参与运算的
        val newRDD: RDD[(String, Int)] = rdd.reduceByKey(
            (x: Int, y: Int) => {
                x + y
            }
        )

        newRDD.collect().foreach(println)


        sc.stop()


    }

}
