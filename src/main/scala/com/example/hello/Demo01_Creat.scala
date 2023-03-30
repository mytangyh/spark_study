package com.example.hello

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo01_Creat {
  def main(args: Array[String]): Unit = {
    //TODO 1.env/准备sc/SparkContext/Spark上下文执行环境
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("Warn")
    //TODO 2.source/读取数据
    val rdd1: RDD[Int] = sc.parallelize(seq = 1 to 10)//4
    val rdd2: RDD[Int] = sc.parallelize(seq = 1 to 10, numSlices = 3)//3
    val rdd3: RDD[Int] = sc.makeRDD(seq = 1 to 10)//4
    val rdd4: RDD[Int] = sc.makeRDD(seq = 1 to 10, numSlices = 3)//3
    //RDD[一行行的数据]
    val rdd5: RDD[String] = sc.textFile("data/input/word.txt") //2
    val rdd6: RDD[String] = sc.textFile("data/input/word.txt", 3) //3
    //RDD[一行行的数据]
    val rdd7: RDD[String] = sc.textFile("data/input/ratings10") //10
    val rdd8: RDD[String] = sc.textFile("data/input/ratings10", 3) //10
    //RDD[(文件名, 一行行的数据),(文件名, 一行行的数据)....]
    val rdd9: RDD[(String, String)] = sc.wholeTextFiles("data/input/ratings10") //2
    val rdd10: RDD[(String, String)] = sc.wholeTextFiles("data/input/ratings10", 3) //3

    println(rdd1.getNumPartitions) //4 //底层partitions.length
    println(rdd2.partitions.length) //3
    println(rdd3.getNumPartitions) //4
    println(rdd4.getNumPartitions) //3
    println(rdd5.getNumPartitions) //2
    println(rdd6.getNumPartitions) //3
    println(rdd7.getNumPartitions) //10
    println(rdd8.getNumPartitions) //10
    println(rdd9.getNumPartitions) //2
    println(rdd10.getNumPartitions) //3

  }
}