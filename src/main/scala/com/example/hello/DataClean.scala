package com.example.hello

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object DataClean {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Load Tar Gz File to Hive")
      //      .enableHiveSupport()
      //      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .master("local[*]")
      .getOrCreate()

    val defaultPartitionNum = spark.conf.get("spark.sql.shuffle.partitions")
    println(defaultPartitionNum)
//    cline.map(_.)





  }

}
