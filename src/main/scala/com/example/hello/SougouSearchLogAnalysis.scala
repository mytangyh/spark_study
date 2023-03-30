package com.example.hello

import com.hankcs.hanlp.HanLP
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object SougouSearchLogAnalysis {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //TODO 1.读取数据
    val lines: RDD[String] = sc.textFile("data/input/SougouQ.sample")

    //TODO 2.数据清洗
    val SougouRecordRDD: RDD[SougouRecord] = lines.map(line => {
      val arr: Array[String] = line.split("\\s+")
      SougouRecord(
        arr(0), arr(1), arr(2), arr(3) toInt, arr(4) toInt, arr(5)
      )
    })
    val wordsRDD: RDD[String] = SougouRecordRDD.flatMap(record => {
      val wordsStr: String = record.queryWords.replaceAll("\\[|\\]", "")
      import scala.collection.JavaConverters._
      HanLP.segment(wordsStr).asScala.map(_.word)
    })


    //TODO 3.数据分析
    //  1.热门搜索
    val result1: Array[(String, Int)] = wordsRDD
      .filter(word => !word.equals(".") && !word.equals("+") && !word.equals("的"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(10)
    //  2.用户热门搜索词
    val userIdAndWordRDD: RDD[(String, String)] = SougouRecordRDD.flatMap(record => {
      val wordsStr: String = record.queryWords.replaceAll("\\[|\\]", "")
      import scala.collection.JavaConverters._
      val words: mutable.Buffer[String] = HanLP.segment(wordsStr).asScala.map(_.word)
      val userId: String = record.userId
      words.map(word => (userId, word))
    })
    val result2: Array[((String, String), Int)] = userIdAndWordRDD
      .filter(t => !t._2.equals(".") && !t._2.equals("+") && !t._2.equals("的"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(10)

    //  3.各个时间段搜索热度
    SougouRecordRDD.map(record => {
      val timeStr: String = record.queryTime
      val hourAndMinStr: String = timeStr.substring(0, 5)
      (hourAndMinStr,1)
    }).reduceByKey(_ + _)
      .sortBy(_._2,false)
      .take(10)
      .foreach(println)
    




    //TODO 4.数据展示
//    result1.foreach(println)
//    result2.foreach(println)

    //TODO 5.关闭环境
    sc.stop()
  }
  //样例类

  /**
   * 用户搜索点击网页记录Record
   *
   * @param queryTime  访问时间，格式为：HH:mm:ss
   * @param userId     用户ID
   * @param queryWords 查询词
   * @param resultRank 该URL在返回结果中的排名
   * @param clickRank  用户点击的顺序号
   * @param clickUrl   用户点击的URL
   */
  case class SougouRecord(
                           queryTime: String,
                           userId: String,
                           queryWords: String,
                           resultRank: Int,
                           clickRank: Int,
                           clickUrl: String
                         )

}
