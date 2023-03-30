package com.example.hello

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo05_Aggregate_NoKey {
  def main(args: Array[String]): Unit = {
    //TODO 1.env/准备sc/SparkContext/Spark上下文执行环境
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("Warn")
    //TODO 2.source/读取数据
    //可增可减
    val rdd1: RDD[Int] = sc.parallelize(1 to 10)//4

    println(rdd1.sum())
    println(rdd1.reduce(_+_))
    println(rdd1.fold(0)(_+_))
    println(rdd1.aggregate(0)(_+_,_+_))



  }
}