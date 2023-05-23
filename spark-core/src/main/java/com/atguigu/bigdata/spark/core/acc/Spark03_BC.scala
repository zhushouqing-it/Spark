package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-20-21:04
 */
object Spark03_BC {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")

        val sc = new SparkContext(sparkConf)

        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("c", 2), ("b", 3)
        ))

        val map = Map(("a", 4), ("c", 5), ("b", 6))

        // 广播变量bc，存放在Executor中，只读，不能更改
        // 封装广播变量
        val bc: Broadcast[Map[String, Int]] = sc.broadcast(map)

//        val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
//            ("a", 4), ("c", 5), ("b", 6)
//        ))

        // join会导致数据量几何增长，并且会影响shuffle的性能，不推荐使用
//        val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
//        joinRDD.collect().foreach(println)

        // ("a", 1), ("c", 2), ("b", 3)
        //(a,(1,4)),(b,(2,5)),(c(3,6))

        rdd1.map{
            case (w,c) => {
                // 访问广播变量。
                val i: Int = bc.value.getOrElse(w, 0)
                (w,(c,i))
            }
        }.collect().foreach(println)


        sc.stop()


    }
}
