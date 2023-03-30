package com.example.hello

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo08_Sort {
  def main(args: Array[String]): Unit = {
    //TODO 1.env/准备sc/SparkContext/Spark上下文执行环境
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[1]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("Warn")
    //TODO 2.source/读取数据

    //RDD[一行行的数据]
    val lines: RDD[String] = sc.textFile("data/input/word.txt") //2
    val result: RDD[(String, Int)] = lines.filter(StringUtils.isNotBlank(_))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    //需求:对WordCount的结果进行排序,取出top3
    val sortResult1: Array[(String, Int)] = result
      .sortBy(_._2, false) //按照数量降序排列
      .take(3)//取出前3个

    //result.map(t=>(t._2,t._1))
    val sortResult2: Array[(Int, String)] = result.map(_.swap)
      .sortByKey(false)//按照数量降序排列
      .take(3)//取出前3个

    val sortResult3: Array[(String, Int)] = result.top(3)(Ordering.by(_._2)) //topN默认就是降序
    //TODO 3.sink
    result.foreach(println)
    println("-----")
    sortResult1.foreach(println)
    println("-----")
    sortResult2.foreach(println)
    println("-----")
    sortResult3.foreach(println)

  }
}