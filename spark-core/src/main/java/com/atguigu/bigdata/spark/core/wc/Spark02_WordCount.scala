package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-03-18-19:43
 */
object Spark02_WordCount {
    def main(args: Array[String]): Unit = {

        // Application 自己写的应用程序
        // Spark框架
        // TODO 建立和Spark框架的连接
        // JDBC : Connection
        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        // TODO 执行业务操作

        // 1. 读取文件 获取一行一行的数据
        // hello world
        val lines = sc.textFile("datas")

        // 2. 将一行一行的单词拆分为一个一个的单词（分词）
        // "hello world" => hello,world
        //    val words = lines.flatMap(
        //      line => line.split(" ")
        //    )
        // 扁平化：将整体拆分成个体的操作
        val words: RDD[String] = lines.flatMap(_.split(" "))


        //
        val wordAndOne: RDD[(String, Int)] = words.map(
            word => (word, 1)
        )


        // 3. 将数据根据单词进行分组，便于统计
        // (hello,1),(hello,1)
        val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordAndOne.groupBy(kv => kv._1)

        // 4. 对分组后的数据进行聚合
        // (hello,1),(hello,1)
        // (hello,2)

        val wordToCount: RDD[(String, Int)] = wordGroup.map {
            kv =>
                kv._2.reduce(
                    (t1, t2) => {
                        (t1._1, t1._2 + t2._2)
                    }
                )
        }

//        val wordToCount: RDD[(String, Int)] = wordGroup.map {
//            case (word, list) => {
//                val tuple: (String, Int) = list.reduce(
//                    (t1, t2) => {
//                        (t1._1, t1._2 + t2._2)
//                    }
//                )
//                tuple
//            }
//        }


        // 5. 将转换结果采集到控制台打印
        val array: Array[(String, Int)] = wordToCount.collect()
        array.foreach(println)

        // TODO 关闭连接
        sc.stop()
    }
}
