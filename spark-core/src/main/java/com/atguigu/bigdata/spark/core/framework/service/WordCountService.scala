package com.atguigu.bigdata.spark.core.framework.service

import com.atguigu.bigdata.spark.core.framework.common.TService
import com.atguigu.bigdata.spark.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * @author ZhuShouqing
 * @create 2023-04-23-19:50
 *         服务层
 */
class WordCountService extends TService{

    private val wordCountDao = new WordCountDao()

    // 数据分析
    def dataAnalysis() = {

        val lines = wordCountDao.readFile("datas/word.txt")

        val words: RDD[String] = lines.flatMap(_.split(" "))

        val wordAndOne: RDD[(String, Int)] = words.map(
            word => (word, 1)
        )

        val wordToCount: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)

        val result: Array[(String, Int)] = wordToCount.collect()

        result
    }

}
