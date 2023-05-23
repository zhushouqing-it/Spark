package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-18-20:17
 */
object Spark05_RDD_Operator_saveAsTextFile {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(
            ("a",1),("a",2),("a",3)
        ))

        rdd.saveAsTextFile("output")
        rdd.saveAsObjectFile("output1")
        //saveAsSequenceFile 方法要求数据为键值对类型
        rdd.saveAsSequenceFile("output2")

        sc.stop()

    }

}
