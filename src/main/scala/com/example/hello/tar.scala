package com.example.hello

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io.{File, FileInputStream}
import java.util.zip.GZIPInputStream
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream

object tar {
  def main(args: Array[String]): Unit = {
    //TODO 1.env/准备sc/SparkContext/Spark上下文执行环境
    val conf: SparkConf = new SparkConf().setAppName("tar").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("Warn")
    //TODO 2.source/读取数据
    val file = new File("data/input/data.tar.gz")
    val fileInputStream = new FileInputStream(file)
    val gzipInputStream = new GZIPInputStream(fileInputStream)
    val tarArchiveInputStream = new TarArchiveInputStream(gzipInputStream)
    var entry = tarArchiveInputStream.getNextTarEntry
//    var rdd: RDD[String] = sc.emptyRDD
    var dd = entry
    var flag = 1
    while (entry != null) {
      if (entry.isDirectory()) {
        println(s"Directory: ${entry.getName}")
      } else {
        println(s"File: ${entry.getName}")
        if (flag == 1){
          val bytes = new Array[Byte](entry.getSize.toInt)
          tarArchiveInputStream.read(bytes)
          val str = new String(bytes)
          println(str)
          val lines = str.split("\n")

          flag = 0
        }
      }
      entry = tarArchiveInputStream.getNextTarEntry()
    }

    fileInputStream.close()
    tarArchiveInputStream.close()
    sc.stop()

  }
}
