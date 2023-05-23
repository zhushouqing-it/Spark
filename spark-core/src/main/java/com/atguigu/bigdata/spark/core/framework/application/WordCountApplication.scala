package com.atguigu.bigdata.spark.core.framework.application

import com.atguigu.bigdata.spark.core.framework.common.TApplication
import com.atguigu.bigdata.spark.core.framework.controller.WordCountController


/**
 * @author ZhuShouqing
 * @create 2023-04-23-19:49
 *         应用层
 */
object WordCountApplication extends App with TApplication {

    // 启动应用程序
    start(){
        val controller = new WordCountController()
        controller.execute()
    }

}
