package com.example.hello

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Demo11_ShareVariable {
  def main(args: Array[String]): Unit = {
    //TODO 1.env/准备sc/SparkContext/Spark上下文执行环境
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[1]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("Warn")
    //TODO 2.source/读取数据
    //需求:
    // 以词频统计WordCount程序为例，处理的数据word2.txt所示，包括非单词符号，
    // 做WordCount的同时统计出特殊字符的数量
    //创建一个计数器/累加器，用于统计特殊字符的数量
    //    sc.textFile("data/input/words2.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).map(x=>{
    //      val key = x._1
    //      val value = x._2
    //      (key,value,StringUtils.countMatches(key,"\\W"))
    //    }).collect().foreach(println)
    //创建一个计数器/累加器
    val mycounter: LongAccumulator = sc.longAccumulator("mycounter")
    //定义一个特殊字符集合
    val ruleList: List[String] = List(",", ".", "!", "#", "$", "%")
    //将集合作为广播变量广播到各个节点
    val broadcast: Broadcast[List[String]] = sc.broadcast(ruleList)
    val lines: RDD[String] = sc.textFile("data/input/words2.txt")
    val WordCuntResult: RDD[(String, Int)] = lines.filter(StringUtils.isNotBlank(_))
      .flatMap(_.split("\\s+"))
      .filter(x => {
        //获取广播变量的值
        val rules: List[String] = broadcast.value
        //判断是否在集合中
        if (rules.contains(x)) {
          //如果在，计数器加1
          mycounter.add(1)
          false
        }
        else {
          true
        }
      }).map((_, 1))
      .reduceByKey(_ + _)
    WordCuntResult.foreach(println)
    println("字符数量"+mycounter.value)



    //TODO 3.sink


  }
}