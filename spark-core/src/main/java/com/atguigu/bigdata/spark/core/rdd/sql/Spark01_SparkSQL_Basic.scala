package com.atguigu.bigdata.spark.core.rdd.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author ZhuShouqing
 * @create 2023-04-24-11:07
 */
object Spark01_SparkSQL_Basic {

    def main(args: Array[String]): Unit = {

        // TODO : 创建SparkSql的运行环境
        val sparkSql: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSql")
        val spark: SparkSession = SparkSession.builder().config(sparkSql).getOrCreate()


        // TODO : 执行逻辑操作

        // DataFrame
        val df: DataFrame = spark.read.json("datas/user.json")
//        df.show()

        // DataFrame ==> Sql
        // 使用df的sql，需要先创建一个临时表，然后用临时表进行操作
        df.createOrReplaceTempView("user")
        spark.sql("select * from user").show
        spark.sql("select age,username from user").show
        spark.sql("select avg(age) from user").show

        // DataFrame ==> DSL ==> df/ds.select(列名).show 列式语法
        // 在使用DataFrame时，如果涉及到转换操作时，需要引入转换规则
        // 使用dsl语法，可以这就用df.select 注意df是弱类型，ds是强类型，df可以用的ds也能用
        import spark.implicits._
        df.select("age", "username").show
        df.select($"age" + 1).show
        df.select('age + 1).show

        //TODO : DataSet   ===> 用于封装强类型，例如自己创建的bean对象，使用场合较多
        // DataFrame 其实就是特定泛型的DataSet，那么DataFrame的操作，DataSet也能用
        val seq = Seq(1, 2, 3, 4)
        val ds: Dataset[Int] = seq.toDS()
        ds.show()


        // RDD <=> DataFrame
        val rdd = spark.sparkContext.makeRDD(List((1, "ZhangSan"), (2, "WangWu"), (3, "LiSi")))
        // 将rdd =》 DataFrame
        val df2: DataFrame = rdd.toDF("id", "name")
        // 将DataFrame =》 rdd
        val rdd1: RDD[Row] = df.rdd

        // DataFrame <=> DataSet
        //DataFrame  => DataSet
        val ds2: Dataset[User] = df2.as[User]
        // DataSet => DataFrame
        val df3: DataFrame = ds2.toDF()

        // RDD <=> DataSet
        val ds3: Dataset[User] = rdd.map {
            case (id, name) => {
                User(id, name)
            }
        }.toDS()

        val userRDD: RDD[User] = ds3.rdd


        // TODO : 关闭环境
        spark.close()


    }

    case class User(id: Int, name: String)
}
