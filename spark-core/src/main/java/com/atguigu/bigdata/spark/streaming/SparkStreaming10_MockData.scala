package com.atguigu.bigdata.spark.streaming

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scala.collection.mutable.ListBuffer
import scala.util.Random


/**
 * @author ZhuShouqing
 * @create 2023-05-14-13:25
 */
object SparkStreaming10_MockData {

    def main(args: Array[String]): Unit = {

        // 生成模拟数据
        // 格式：timestamp、area、city、userid、adid
        // 含义：时间戳、区域、城市、用户、广告



        // Application ==>  kafka ==> SparkStreaming ==> Analysis
        // 创建配置对象
        val prop = new Properties()
        // 添加配置
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop177:9092")
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer")
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String,String](prop)

        while (true) {
            mockData().foreach(
                data => {
                    // 向Kafka中生成数据
                    val record = new ProducerRecord[String,String]("atguiguNew",data)
                    producer.send(record)
                    print(data)
                }
            )


            Thread.sleep(2000)
        }


    }

    def mockData() = {
        val list = ListBuffer[String]()
        val areaList = ListBuffer[String]("华北", "华东", "华南")
        val cityList = ListBuffer[String]("北京", "上海", "深圳")
        for (i <- 1 to 30) {
            val area = areaList(new Random().nextInt(3))
            val city = cityList(new Random().nextInt(3))
            val userid = new Random().nextInt(6)+1
            val adid = new Random().nextInt(6)+1
            list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")
        }
        list
    }

}
