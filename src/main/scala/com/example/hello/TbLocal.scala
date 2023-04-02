package com.example.hello

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.{File, FileInputStream}
import java.util
import java.util.zip.GZIPInputStream
import scala.collection.mutable.ArrayBuffer

object TbLocal {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Load Tar Gz File to Hive")
//      .enableHiveSupport()
//      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .master("local[*]")
      .getOrCreate()

    val file = new File("data/input/tb.tar.gz")
    val fileInputStream = new FileInputStream(file)
    val gzipInputStream = new GZIPInputStream(fileInputStream)
    val tarArchiveInputStream = new TarArchiveInputStream(gzipInputStream)

    // 缓存读取
    val blockSize = 64 * 1024 * 1024 // 分块大小，1MB
    val bytes = new Array[Byte](blockSize)
    val buffer = new ArrayBuffer[String]()
    val linesLen = new util.ArrayList[String]()
    var entry = tarArchiveInputStream.getNextTarEntry
    while (entry != null) {
      if (!entry.isDirectory && entry.getSize > 0) {
        println(s"File: ${entry.getName}")
        var bytesRead = 0
        buffer.clear()
        while ( {
          bytesRead = tarArchiveInputStream.read(bytes)
          bytesRead != -1
        }) {
          buffer.append(new String(bytes, 0, bytesRead))
        }
        val str = buffer.mkString("")
        val lines = str.split("\n")
        println(s"Number of lines: ${lines.length}")
        linesLen.add(entry.getName + ":" + lines.length.toString)

        // 每行数据的字段去掉单引号并以制表符分割
//        val cleanedLines = lines.map(_.replaceAll("'", "").trim.split("\t")).filter(_.length == 11)
//
//        // 将字段转换为DataFrame
//        val rdd = spark.sparkContext.parallelize(cleanedLines)
//          .map(fields => (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8), fields(9).toFloat, fields(10), fields(0).substring(0, 6)))
//        import spark.implicits._
//        val df = rdd.toDF("Transaction_Date", "Transaction_Time_Id", "Process_Date", "Process_Date_Id", "Card_Type", "Entry_Address ", "Exit_Address", "Line_Id", "Pasgr_Type", "Transaction_Cnt", "Card_Issuer", "partition_month")
//        df.printSchema()
//                df.show(10)
//        df.write.mode(SaveMode.Append).partitionBy("partition_month").insertInto("acc.ods_acc_cardSort_15min")
        //        df.createOrReplaceTempView("tmp_table")
        //        spark.sql("INSERT INTO acc.ods_acc_cardSort_15min PARTITION(partition_month) SELECT * FROM tmp_table")
        System.gc() // 手动调用垃圾回收
      }
      entry = tarArchiveInputStream.getNextTarEntry
    }
    fileInputStream.close()
    tarArchiveInputStream.close()

    import java.io._
    val pw = new PrintWriter(new File("data/output/tbtest.txt"))
    for (i <- 0 until linesLen.size()) {
      pw.write(linesLen.get(i) + "")
      pw.write("\r")
    }
    pw.close()
  }

}
