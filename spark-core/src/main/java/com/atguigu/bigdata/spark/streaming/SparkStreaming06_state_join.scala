package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author ZhuShouqing
 * @create 2023-05-14-13:25
 */
object SparkStreaming06_state_join {

    def main(args: Array[String]): Unit = {


        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

        val ssc = new StreamingContext(sparkConf, Seconds(3))


        val data9999: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        val data8888: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)

        val map9999: DStream[(String, Int)] = data9999.map((_, 9))
        val map8888: DStream[(String, Int)] = data8888.map((_, 8))


        val joinDS: DStream[(String, (Int, Int))] = map9999.join(map8888)

        joinDS.print()


        ssc.start()

        ssc.awaitTermination()


    }

}
