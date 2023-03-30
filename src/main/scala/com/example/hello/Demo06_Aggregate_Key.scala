package com.example.hello

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo06_Aggregate_Key {
  def main(args: Array[String]): Unit = {
    //TODO 1.env/准备sc/SparkContext/Spark上下文执行环境
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("Warn")
    //TODO 2.source/读取数据
    //可增可减
    val lines: RDD[String] = sc.textFile("data/input/word.txt") //2
    val wordAndOneRDD: RDD[(String, Int)] = lines.filter(StringUtils.isNotBlank(_))
      .flatMap(_.split(" "))
      .map((_, 1))
    //聚合
    val grouped: RDD[(String, Iterable[Int])] = wordAndOneRDD.groupByKey()
    //    wordAndOneRDD.groupBy(_,1)
    val result: RDD[(String, Int)] = grouped.mapValues(_.sum)
    result.foreach(println)

    val result2: RDD[(String, Int)] = wordAndOneRDD.reduceByKey(_ + _)
    result2.foreach(println)

    val result3: RDD[(String, Int)] = wordAndOneRDD.foldByKey(0)(_ + _)
    result3.foreach(println)



  }
}