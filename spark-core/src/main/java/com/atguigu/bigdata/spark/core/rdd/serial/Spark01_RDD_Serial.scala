package com.atguigu.bigdata.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-19-12:40
 */
object Spark01_RDD_Serial {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Serial")
        val sc = new SparkContext(conf)

        val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

        val search = new Search("h")

//        search.getMatch1(rdd).collect().foreach(println)
        search.getMatch2(rdd).collect().foreach(println )

        sc.stop()

    }

    // 查询对象
    // 类的构造参数其实就是类的属性.构造参数需要进行闭包检测，其实就等同于类进行闭包检测
    class Search(query: String) extends Serializable {
        def isMathch(s: String): Boolean = {
            s.contains(query)
        }

        // 函数序列化案例
        def getMatch1(rdd: RDD[String]): RDD[String] = {
            // rdd.filter(this.isMatch)
            rdd.filter(isMathch)
        }

        // 属性序列化案例
        def getMatch2(rdd: RDD[String]): RDD[String] = {
            // rdd.filter(x => x.contains(this.query))
            rdd.filter(x => x.contains(query))
//            val q=query
//            rdd.filter(x => x.contains(q))
        }

    }

}
