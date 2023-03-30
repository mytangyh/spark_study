package com.example.hello

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DataClean {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DataClean").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val lines: RDD[String] = sc.textFile("data/input/test.sql")
    val cline: RDD[String] = lines.filter(_.contains("VALUES"))
//    cline.map(_.)





  }

}
