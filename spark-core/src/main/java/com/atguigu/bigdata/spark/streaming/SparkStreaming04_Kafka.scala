package com.atguigu.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author ZhuShouqing
 * @create 2023-05-14-13:25
 */
object SparkStreaming04_Kafka {

    def main(args: Array[String]): Unit = {


        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop177:9092,hadoop178:9092,hadoop179:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )


        val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("atguigu"), kafkaPara)
        )

        kafkaDataDS.map(_.value()).print()


        ssc.start()

        ssc.awaitTermination()


    }

}
