package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ZhuShouqing
 * @create 2023-04-18-20:17
 */
object Spark06_RDD_Operator_foreach_1 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))


        val user = new User()

        // Caused by: java.io.NotSerializableException: com.atguigu.bigdata.spark.core.rdd.operator.action.Spark06_RDD_Operator_foreach_1$User
        // Serialization stack:
        // RDD 算子中传递的函数是会包含闭包操作，那么就会进行检测功能
        // 闭包检测，闭包将函数外部的变量引入自己的内部，形成一个闭包，改变了变量或者对象的生命周期
        rdd.foreach(
            num => {
                println("age=" + (user.age + num))
            }
        )

        sc.stop()

    }

    // class User extends Serializable {
    // 样例类在编译时，会自动混入序列化特质（实现可序列化接口）
    case class User() {
        var age: Int = 30
    }

}
