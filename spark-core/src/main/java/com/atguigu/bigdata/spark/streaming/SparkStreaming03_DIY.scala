package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.parallel.mutable
import scala.util.Random

/**
 * @author ZhuShouqing
 * @create 2023-05-14-13:25
 */
object SparkStreaming03_DIY {

    def main(args: Array[String]): Unit = {


        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

        val ssc = new StreamingContext(sparkConf, Seconds(3))


        val messageDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver())


        messageDS.print()

        ssc.start()

        ssc.awaitTermination()

    }

    /*
    自定义数据采集器
    1、继承Receiver，定义泛型
    2、重写方法
     */
    class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
        private var flag = true

        override def onStart(): Unit = {

            new Thread(
                new Runnable {
                    override def run(): Unit = {
                        while (flag) {
                            val message = "采集的数据为：" + new Random().nextInt(10).toString
                            store(message)
                            Thread.sleep(500)
                        }
                    }
                }
            ).start()

        }

        override def onStop(): Unit = {
            flag = false
        }
    }

}
