package com.atguigu.bigdata.spark.core.framework.controller

import com.atguigu.bigdata.spark.core.framework.common.TController
import com.atguigu.bigdata.spark.core.framework.service.WordCountService


/**
 * @author ZhuShouqing
 * @create 2023-04-23-19:50
 *         控制层
 */
class WordCountController extends TController{

    private val wordCountService = new WordCountService()

    // 调度
    def execute(): Unit = {
        val result: Array[(String, Int)] = wordCountService.dataAnalysis()
        result.foreach(println)

    }

}
