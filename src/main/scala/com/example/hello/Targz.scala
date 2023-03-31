package com.example.hello
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.io.{ByteArrayInputStream, File, FileInputStream}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
object Targz {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Load Tar Gz File to Hive")
      .enableHiveSupport()
//      .master("local[*]")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    // 读取本地文件
    val rdd = spark.sparkContext.binaryFiles("hdfs://node01.hadoop.com:8020/shenzhen/ta.tar.gz")
      .flatMap { case (_, stream) =>
        val bytes = stream.toArray()
        val tarInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new ByteArrayInputStream(bytes)))
        var entry = tarInputStream.getNextEntry
        val buffer = new ArrayBuffer[String]()
        while (entry != null) {
          if (!entry.isDirectory) {
            println(s"File: ${entry.getName}")
            val content = Source.fromInputStream(tarInputStream).getLines().mkString("\n")
            buffer += content
          }
          entry = tarInputStream.getNextEntry
        }
        tarInputStream.close()
        buffer.iterator
      }

      // 转换成DataFrame
      import spark.implicits._
    val df = rdd.flatMap(line => line.split("\n"))
      .map(line => {
        val fields = line.replaceAll("'","").split("\t")
        val partition_month = fields(0).substring(0, 6)
        (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6),fields(7).substring(0,1).toInt, fields(8), partition_month)
      }).toDF("Transaction_Date", "Transaction_Time_Id", "Process_Date", "Process_Time_Id", "Card_Type", "Entry_Address ", "Exit_Address", "Transaction_Cnt", "Card_Issuer", "partition_month")
    // 写入Hive
    df.write.mode(SaveMode.Append)
      .format("orc")
      .insertInto("acc.ods_acc_od_15min")
    spark.stop()

  }

}

/*
SPARK_HOME=/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/spark
${SPARK_HOME}/bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 3g \
--executor-memory 3g \
--num-executors 8 \
--class com.example.hello.Targz \
/root/Hadoopcoding/original-spark_study-1.0-SNAPSHOT.jar
 */