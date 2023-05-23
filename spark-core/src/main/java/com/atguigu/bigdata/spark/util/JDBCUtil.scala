package com.atguigu.bigdata.spark.util

import com.alibaba.druid.pool.DruidDataSourceFactory
import java.sql.Connection
import java.util.Properties
import javax.sql.DataSource


/**
 * @author ZhuShouqing
 * @create 2023-05-15-11:54
 */
object JDBCUtil {

    // 初始化连接池
    var dataSource: DataSource = init()

    def init(): DataSource = {
        val properties = new Properties()
        properties.setProperty("driverClassName", "com.mysql.cj.jdbc.Driver")
        properties.setProperty("url", "jdbc:mysql://hadoop177:3306/spark-streaming?characterEncoding=utf-8&serverTimezone=UTC")
        properties.setProperty("username", "root")
        properties.setProperty("password", "123456")
        properties.setProperty("maxActive", "50")
        properties.setProperty("useSSL","false")
        DruidDataSourceFactory.createDataSource(properties)

    }

    // 获取Mysql的连接
    def getConnection: Connection = {
        dataSource.getConnection
    }

}
