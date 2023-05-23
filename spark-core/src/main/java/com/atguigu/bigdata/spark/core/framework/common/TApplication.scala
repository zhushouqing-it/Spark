package com.atguigu.bigdata.spark.core.framework.common

import com.atguigu.bigdata.spark.core.framework.controller.WordCountController
import com.atguigu.bigdata.spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-23-20:33
 */
trait TApplication {

    def start(master: String = "local[*]", app: String = "Application")(op: => Unit): Unit = {
        val sparConf = new SparkConf().setMaster(master).setAppName(app)
        val sc = new SparkContext(sparConf)
        EnvUtil.put(sc)


        try {
            op
        } catch {
            case ex => println(ex.getMessage)
        }

        sc.stop()
        EnvUtil.clear()

    }

}
