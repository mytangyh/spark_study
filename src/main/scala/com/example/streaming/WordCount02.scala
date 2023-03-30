package com.example.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount02 {
  def main(args: Array[String]): Unit = {
    //TODO 0. 准备环境
    val conf: SparkConf = new SparkConf().setAppName("spark").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./ckp")


    //TODO 1. 读取数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("10.25.67.80", 9999)



    //TODO 2. 处理数据
    //定义一个函数用来处理状态:把当前数据和历史状态进行累加
    //currentValues:表示该key(如:spark)的当前批次的值,如:[1,1]
    //historyValue:表示该key(如:spark)的历史值,第一次是0,后面就是之前的累加值如1
    val updateFunc = (currentValues: Seq[Int], historyValue: Option[Int]) => {
      if (currentValues.size > 0) {
        val currentResult: Int = currentValues.sum + historyValue.getOrElse(0)
        Some(currentResult)
      } else {
        historyValue
      }
    }
    val resultDS: DStream[(String, Int)] = lines.flatMap(_.split(" "))
      .map((_, 1))
//      .reduceByKey(_ + _)
      .updateStateByKey(updateFunc)

    //TODO 3. 输出数据
    resultDS.print()

    //TODO 4. 启动任务
    ssc.start()
    ssc.awaitTermination()

    //TODO 5. 关闭资源
    ssc.stop(stopSparkContext = true, stopGracefully = true)//优雅关闭
  }

}
