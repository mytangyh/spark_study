package com.example.hello

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //TODO 1.env/准备sc/SparkContext/Spark上下文执行环境
    if (args.length != 2) {
      println("Usage:WordCount <input> <output>")
      System.exit(1)
    }
    val conf: SparkConf = new SparkConf().setAppName("wc")//.setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("Warn")
    //TODO 2.source/读取数据
    val lines: RDD[String] = sc.textFile(args(0))
    //TODO 3.transformation/数据操作/转换
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordAndOnes: RDD[(String, Int)] = words.map((_, 1))
    //分组聚合
    val result: RDD[(String, Int)] = wordAndOnes.reduceByKey(_ + _)
    //TODO 4.sink/输出
    //直接输出
//    result.foreach(println)
    //收集为本地集合在输出
//    println(result.collect().toBuffer)
    //输出到路径
    result.repartition(1).saveAsTextFile(args(1))
    sc.stop()

  }
}
