package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark12_RDD_Operator_Transform_sortBy_1 {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - sortBy
        // 分区内有序
        val rdd = sc.makeRDD(List(
            ("1", 1), ("11", 2), ("2", 3)
        ), 2)

        // sortBy方法可以根据指定的规则对数据源中的数据进行排序
        // 默认为升序，但是第二个参数可以改变排序的顺序，false表示降序
        // sortBy 默认情况下，不会改变分区，但是中间存在shuffle操作。
        val newRDD: RDD[(String, Int)] = rdd.sortBy(
            t => t._1,false
        )

        newRDD.collect().foreach(println)


        sc.stop()


    }

}
