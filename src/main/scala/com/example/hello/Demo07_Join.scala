package com.example.hello

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo07_Join {
  def main(args: Array[String]): Unit = {
    //TODO 1.env/准备sc/SparkContext/Spark上下文执行环境
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("Warn")
    //TODO 2.source/读取数据
    //可增可减
    //员工集合:RDD[(部门编号, 员工姓名)]
    val empRDD: RDD[(Int, String)] = sc.parallelize(
      Seq((1001, "zhangsan"), (1002, "lisi"), (1003, "wangwu"))
    )
    //部门集合:RDD[(部门编号, 部门名称)]
    val deptRDD: RDD[(Int, String)] = sc.parallelize(
      Seq((1001, "销售部"), (1002, "技术部"), (1004, "客服部"))
    )
    //TODO 3.transformation
    //需求:求员工对应的部门名称
    val value1: RDD[(Int, (String, String))] = empRDD.join(deptRDD)
    val value2: RDD[(Int, (String, Option[String]))] = empRDD.leftOuterJoin(deptRDD)
    val value3: RDD[(Int, (Option[String], String))] = empRDD.rightOuterJoin(deptRDD)

    value1.foreach(println)
    println("---------")
    value2.foreach(println)
    println("---------")
    value3.foreach(println)



  }
}