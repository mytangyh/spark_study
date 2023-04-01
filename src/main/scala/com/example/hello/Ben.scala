package com.example.hello

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.io.{File, FileInputStream}
import java.util
import java.util.zip.GZIPInputStream
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break

object Ben {
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Load Tar Gz File to Hive")
//      .enableHiveSupport()
//      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .master("local[*]")
      .getOrCreate()

    val file = new File("data/input/ta.tar.gz")
    val fileInputStream = new FileInputStream(file)
    val gzipInputStream = new GZIPInputStream(fileInputStream)
    val tarArchiveInputStream = new TarArchiveInputStream(gzipInputStream)

    // 缓存读取
    val blockSize = 1024 * 1024 // 分块大小，1MB
    val bytes = new Array[Byte](blockSize)
    val buffer = new ArrayBuffer[String]()
    var linesLen = new util.ArrayList[String]()
    var entry = tarArchiveInputStream.getNextTarEntry
    while (entry != null) {
      if (!entry.isDirectory) {
//        println(s"File: ${entry.getName}")
        logger.info(s"File: ${entry.getName}")
        var bytesRead = 0
        buffer.clear()
        while ({bytesRead = tarArchiveInputStream.read(bytes); bytesRead != -1}) {
          buffer.append(new String(bytes, 0, bytesRead))
        }
        val str = buffer.mkString("")
        val lines = str.split("\n")
//        println(s"Number of lines: ${lines.length}")
        logger.info(s"Number of lines: ${lines.length}")
        linesLen.add(entry.getName+":" +lines.length.toString)


       System.gc() // 手动调用垃圾回收
      }
      entry = tarArchiveInputStream.getNextTarEntry
    }
    fileInputStream.close()
    tarArchiveInputStream.close()
    //把linesLen写入文件
    import java.io._
    val pw = new PrintWriter(new File("data/output/linesLen.txt" ))
    for (i <- 0 until linesLen.size()) {
      pw.write(linesLen.get(i) + "")
      pw.write("\r")
    }
    pw.close()
  }
}


