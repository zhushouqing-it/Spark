package com.atguigu.bigdata.spark.core.framework.common

import com.atguigu.bigdata.spark.core.framework.util.EnvUtil

/**
 * @author ZhuShouqing
 * @create 2023-04-23-20:47
 */
trait TDao {

    def readFile(path: String) ={

        EnvUtil.take().textFile(path)

    }

}
