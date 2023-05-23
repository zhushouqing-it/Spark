package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}


/**
 * @author ZhuShouqing
 * @create 2023-05-14-13:25
 */
object SparkStreaming08_Close {

    def main(args: Array[String]): Unit = {

        /*
        线程的关闭：
        val thread = new Thread()
        thread.start()

        thread.stop() : // 强制关闭
         */



        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

        val ssc = new StreamingContext(sparkConf, Seconds(3))


        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

        // 窗口的范围应该是采集周期的整数倍
        // 窗口可以滑动的，但是默认情况下，一个采集周期进行滑动
        // 这样的话，可能会出现重复数据的计算，为了避免这种情况，可以改变滑动的步长
        val windowDS: DStream[(String, Int)] = wordToOne.window(Seconds(6), Seconds(6))

        val wordToCount: DStream[(String, Int)] = windowDS.reduceByKey(_ + _)

        wordToCount.print()



        ssc.start()

        // 如果想要关闭采集器，那么需要创建新的线程
        new Thread(
            new Runnable {
                override def run(): Unit = {

                    Thread.sleep(5000)
                    val state: StreamingContextState = ssc.getState()
                    if (state == StreamingContextState.ACTIVE) {
                        // 优雅的关闭
                        // 计算节点不在接收新的数据，而是将现有的数据处理完毕，然后关闭
                        ssc.stop(true,true)
                    }
                    System.exit(0)
                }
            }
        ).start()


        ssc.awaitTermination()  // block 阻塞main线程



    }

}
