package com.example.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount01 {
  def main(args: Array[String]): Unit = {
    //TODO 0. 准备环境
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))


    //TODO 1. 读取数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("10.25.67.80", 9999)



    //TODO 2. 处理数据
    val resultDS: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //TODO 3. 输出数据
    resultDS.print()

    //TODO 4. 启动任务
    ssc.start()
    ssc.awaitTermination()

    //TODO 5. 关闭资源
    ssc.stop(stopSparkContext = true, stopGracefully = true)//优雅关闭
  }

}
