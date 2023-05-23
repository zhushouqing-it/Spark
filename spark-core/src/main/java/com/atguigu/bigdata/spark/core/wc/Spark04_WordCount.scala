package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.parallel.mutable
import scala.collection.parallel.mutable.ParMap

/**
 * @author ZhuShouqing
 * @create 2023-03-18-19:43
 */
object Spark04_WordCount {
    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)


        sc.stop()
    }

    // groupBy
    def wordCount1(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("hello scala", "hello spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
        val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)

    }

    // groupByKey
    def wordCount2(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("hello scala", "hello spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordOne: RDD[(String, Int)] = words.map(word => (word, 1))
        val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
        val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)

    }

    // reduceByKey
    def wordCount3(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("hello scala", "hello spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordOne: RDD[(String, Int)] = words.map(word => (word, 1))
        val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)

    }

    // aggregateByKey
    def wordCount4(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("hello scala", "hello spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordOne: RDD[(String, Int)] = words.map(word => (word, 1))
        val wordCount: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_ + _, _ + _)

    }

    // foldByKey
    def wordCount5(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("hello scala", "hello spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordOne: RDD[(String, Int)] = words.map(word => (word, 1))
        val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)(_ + _)

    }


    // combineByKey
    def wordCount6(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("hello scala", "hello spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordOne: RDD[(String, Int)] = words.map(word => (word, 1))
        val wordCount: RDD[(String, Int)] = wordOne.combineByKey(
            v => v,
            (x:Int,y:Int) => x+y,
            (x:Int,y:Int) => x+y
        )

    }

    // countByKey
    def wordCount7(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("hello scala", "hello spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordOne: RDD[(String, Int)] = words.map(word => (word, 1))
        val wordCount: collection.Map[String, Long] = wordOne.countByKey()

    }

    // countByValue
    def wordCount8(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("hello scala", "hello spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordCount: collection.Map[String, Long] = words.countByValue()

    }

    // reduce
    def wordCount9(sc: SparkContext): Unit = {
        val rdd: RDD[String] = sc.makeRDD(List("hello scala", "hello spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val mapWord: RDD[ParMap[String, Long]] = words.map(
            word => {
                mutable.ParMap[String, Long]((word, 1))
            }
        )

        val wordCount: ParMap[String, Long] = mapWord.reduce(
            (map1, map2) => {
                map2.foreach {
                    case (word, count) => {
                        val newCount: Long = map1.getOrElse(word, 0L) + count
                        map1.updated(word, newCount)
                    }
                }
                map1
            }
        )

        println(wordCount)
    }


}
