package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark23_RDD_Operator_Transform_cogroup {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)

        val rdd1 = sc.makeRDD(List(
            ("a",1),("b",2)//,("c",3)
        ))

        val rdd2 = sc.makeRDD(List(
            ("a",4),("b",5),("c",6),("c",7)
        ))

        // cogroup : connect + group (分组连接)
        val coGroupRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
        coGroupRDD.collect().foreach(println)
        sc.stop()


    }

}
