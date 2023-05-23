package com.atguigu.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-01-15:36
 */
object Spark06_RDD_Operator_Transform_groupby_Test {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(conf)


        // TODO 算子 - groupBy
        // 统计apache.log 该文件的每个时间段的访问量

        val rdd: RDD[String] = sc.textFile("datas/apache.log")

        val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(
            line => {
                val datas: Array[String] = line.split(" ")
                val time: String = datas(3)
                //time.substring()
                // 获取格式化的对象，然后利用这个格式化的对象对我们获取到的time进行解析成日期格式
                val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                // 将我们获取到的time字符串按照他的规则格式化为日期
                val date: Date = sdf.parse(time)
                val sdf1 = new SimpleDateFormat("HH")
                // 利用format获取到日期的时
                val hour: String = sdf1.format(date)
                (hour, 1)

            }
        ).groupBy(_._1)


//        timeRDD.map {
//        case (hour, iter) => {
//            (hour, iter.size)
//        }
//    }.collect().foreach(println)
        timeRDD.map(
            iter => (iter._1, iter._2.size)
        ).collect().foreach(println)


        sc.stop()


    }

}
