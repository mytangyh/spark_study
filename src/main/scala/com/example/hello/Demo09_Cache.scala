package com.example.hello

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Demo09_Cache {
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
    //TODO =====注意:resultRDD在后续会被频繁使用到,且该RDD的计算过程比较复杂,所以为了提高后续访问该RDD的效率,应该将该RDD放到缓存中
    //result.cache()//底层persist()
    //result.persist()//底层persist(StorageLevel.MEMORY_ONLY)
    //需求:对WordCount的结果进行排序,取出top3
    result.persist(StorageLevel.MEMORY_AND_DISK)
    //TODO =====注意:上面的缓存持久化并不能保证RDD数据的绝对安全,所以应使用Checkpoint把数据发在HDFS上
    sc.setCheckpointDir("./ckp")//实际中写HDFS目录
    result.checkpoint()

    val sortResult1: Array[(String, Int)] = result
      .sortBy(_._2, false) //按照数量降序排列
      .take(3)//取出前3个

    //result.map(t=>(t._2,t._1))
    val sortResult2: Array[(Int, String)] = result.map(_.swap)
      .sortByKey(false)//按照数量降序排列
      .take(3)//取出前3个

    val sortResult3: Array[(String, Int)] = result.top(3)(Ordering.by(_._2)) //topN默认就是降序
    //TODO 3.sink
    result.unpersist()//释放缓存中的数据
    result.foreach(println)
    println("-----")
    sortResult1.foreach(println)
    println("-----")
    sortResult2.foreach(println)
    println("-----")
    sortResult3.foreach(println)

  }
}