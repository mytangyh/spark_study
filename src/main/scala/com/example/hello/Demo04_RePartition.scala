package com.example.hello

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo04_RePartition {
  def main(args: Array[String]): Unit = {
    //TODO 1.env/准备sc/SparkContext/Spark上下文执行环境
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("Warn")
    //TODO 2.source/读取数据
    //可增可减
    val rdd1: RDD[Int] = sc.parallelize(1 to 10)//4
    val rdd2: RDD[Int] = rdd1.repartition(5)
    val rdd3: RDD[Int] = rdd1.repartition(3)

    println(rdd1.getNumPartitions)
    println(rdd2.getNumPartitions)
    println(rdd3.getNumPartitions)
    //coalesce 默认只能减少
    val rdd4: RDD[Int] = rdd1.coalesce(5)
    val rdd5: RDD[Int] = rdd1.coalesce(3)

    println(rdd4.getNumPartitions)
    println(rdd5.getNumPartitions)



  }
}