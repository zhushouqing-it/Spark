package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark22_RDD_Operator_Transform_leftJoin {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)

        val rdd1 = sc.makeRDD(List(
            ("a",1),("b",2),("c",3)
        ))

        val rdd2 = sc.makeRDD(List(
            ("a",4),("b",5)//,("c",6)
        ))

        val leftJoinRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
        val rightJoinRDD: RDD[(String, (Option[Int],Int))] = rdd1.rightOuterJoin(rdd2)

        leftJoinRDD.collect().foreach(println)
        rightJoinRDD.collect().foreach(println)

        sc.stop()


    }

}
