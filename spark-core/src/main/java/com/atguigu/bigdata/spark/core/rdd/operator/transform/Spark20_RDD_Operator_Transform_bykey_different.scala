package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark20_RDD_Operator_Transform_bykey_different {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - combineByKey
        // 将数据根据不同的规则进行分区内计算和分区间计算
        // 该算子的功能主要是为了 解决reduceByKey只能让分区内和分区间采用同一种规则计算

        val rdd: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1), ("a", 2), ("b", 3),
                ("b", 4), ("b", 5), ("a", 6),
            ), 2
        )


        /*
        reduceByKey:
            combineByKeyWithClassTag[V](
                (v: V) => v, // 第一个值不会参与计算
                func,        // 分区内计算规则
                func,         // 分区间的计算规则
                // 分区内默认和分区间是相同的
                )

        aggregateByKey:
                combineByKeyWithClassTag[U](
                    (v: V) => cleanedSeqOp(createZero(), v), // 初始值和第一个key的value值进行分区内的数据操作
                    cleanedSeqOp, // 分区内计算规则
                    combOp,       // 分区间的计算规则
                    )


        foldByKey:
               combineByKeyWithClassTag[V](
                   (v: V) => cleanedFunc(createZero(), v), // 初始值和第一个key的value值进行分区内的数据操作
                   cleanedFunc, // 分区内计算规则
                   cleanedFunc, // 分区间计算规则
                   // 分区内默认和分区间是相同的
                   )

         combineByKey:
                combineByKeyWithClassTag(
                    createCombiner, // 相同key的第一条数据进行的处理
                    mergeValue,     // 表示分区内数据的处理函数
                    mergeCombiners, // 表示分区间的数据的处理函数
         */

        rdd.reduceByKey(_+_)//wordCount
        rdd.aggregateByKey(0)(_+_,_+_)  //wordCount
        rdd.foldByKey(0)(_+_)  //wordCount
        rdd.combineByKey(v=>v,(x:Int,y:Int)=> x+y,(x:Int,y:Int)=> x+y)  //wordCount




        sc.stop()


    }

}
