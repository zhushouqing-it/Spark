package com.atguigu.bigdata.spark.core.framework.util

import org.apache.spark.SparkContext

/**
 * @author ZhuShouqing
 * @create 2023-04-23-21:01
 */
object EnvUtil {

    private val scLocal = new ThreadLocal [SparkContext]()

    def put(sc: SparkContext): Unit = {
        scLocal.set(sc)
    }

    def take(): SparkContext = {
        scLocal.get()
    }

    def clear(): Unit = {

        scLocal.remove()

    }

}
