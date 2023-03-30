package com.example.hello

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo03_PartitionOperation {
  def main(args: Array[String]): Unit = {
    //TODO 1.env/准备sc/SparkContext/Spark上下文执行环境
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("Warn")
    //TODO 2.source/读取数据

    //RDD[一行行的数据]
    val lines: RDD[String] = sc.textFile("data/input/word.txt") //2
    val result: RDD[(String, Int)] = lines.filter(StringUtils.isNotBlank(_))
      .flatMap(_.split(" "))
//      .map((_, 1))
      .mapPartitions(iter=>{
        iter.map((_,1))
      })
      .reduceByKey(_ + _)
    //TODO 3.sink
//    result.foreach(println)
    result.foreachPartition(iter=>{
      iter.foreach(println)
    })
    result.saveAsTextFile("data/output/result1")

  }
}