package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark17_RDD_Operator_Transform_aggregateByKey {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - aggregateByKey
        // 将数据根据不同的规则进行分区内计算和分区间计算
        // 该算子的功能主要是为了 解决reduceByKey只能让分区内和分区间采用同一种规则计算

        val rdd: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1), ("a", 2), ("a", 3), ("a", 4)
            ),2
        )

        // (a,[1,2]),(a,[3,4])
        // (a,2),(a,4)
        // (a,6)
        // aggregateByKey 存在函数柯里化，有两个参数列表
        // 第一个参数列表 ,需要传递一个参数，表示为初始值
        //         主要用于当碰见第一个key的时候，和value进行分区内计算
        // 第二个参数列表，需要传递2个参数
        //      第一个参数表示分区内计算规则
        //      第二个参数表示分区间的计算规则
        rdd.aggregateByKey(0)(
            (x,y) => math.max(x,y),
            (x,y) => x+y
        ).collect().foreach(println)


        // 如果聚合计算时，分区内和分区间的计算规则相同，spark提供了简化的方法
        rdd.foldByKey(0)(_+_).collect().foreach(println)

        sc.stop()


    }

}
