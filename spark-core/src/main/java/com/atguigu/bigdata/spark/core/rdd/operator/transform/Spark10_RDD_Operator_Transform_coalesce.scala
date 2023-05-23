package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark10_RDD_Operator_Transform_coalesce {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - coalesce
        // 根据数据量缩减分区，用于大数据过滤后，提高小数据集的执行效率
        // 当spark程序中存在过多的小任务的时候，可以通过coalesce方法，收缩合并分区，
        // 减少分区的个数，减少任务调度成本。
        val rdd = sc.makeRDD(List(1,2,3,4,5,6),3)

        // coalesce方法默认情况下不会将分区的数据打乱重新组合
        // （1，2）  （3，4，5，6）
        // 这种情况下的缩减分区，可能会导致数据不均衡，出现数据倾斜
        // 如果想要让数据均衡，可以进行shuffle处理
        val newRDD: RDD[Int] = rdd.coalesce(2,true)

        newRDD.saveAsTextFile("output")



        sc.stop()


    }

}
