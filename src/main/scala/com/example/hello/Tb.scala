package com.example.hello

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.zip.GZIPInputStream
import scala.collection.mutable.ArrayBuffer


object Tb {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Load Tar Gz File to Hive")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    val filePath = new Path("hdfs://node01.hadoop.com:8020/shenzhen/tb.tar.gz")
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val inputStream = fileSystem.open(filePath)
    val gzipInputStream = new GZIPInputStream(inputStream)
    val tarArchiveInputStream = new TarArchiveInputStream(gzipInputStream)

    // 缓存读取
    val blockSize = 128 * 1024 * 1024 // 分块大小，128MB
    val bytes = new Array[Byte](blockSize)
    val buffer = new ArrayBuffer[String]()

    var entry = tarArchiveInputStream.getNextTarEntry
    entry = tarArchiveInputStream.getNextTarEntry
    entry = tarArchiveInputStream.getNextTarEntry
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

        // 每行数据的字段去掉单引号并以制表符分割
        val cleanedLines = lines.map(_.replaceAll("'", "").trim.split("\t")).filter(_.nonEmpty)

        // 将字段转换为DataFrame
        val rdd = spark.sparkContext.parallelize(cleanedLines)
          .map(fields => (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8), fields(9).toFloat, fields(10), fields(0).substring(0, 6)))
        import spark.implicits._
        val df = rdd.toDF("Transaction_Date", "Transaction_Time_Id", "Process_Date", "Process_Date_Id", "Card_Type", "Entry_Address ", "Exit_Address", "Line_Id", "Pasgr_Type", "Transaction_Cnt", "Card_Issuer", "partition_month")
        //        df.show()
//        if (df.count() > 0) {
//          df.write.mode(SaveMode.Append).insertInto("acc.ods_acc_cardSort")
//        }
                df.createOrReplaceTempView("tmp_table")
                spark.sql("INSERT INTO acc.ods_acc_cardSort PARTITION(partition_month) SELECT * FROM tmp_table")
//        System.gc() // 手动调用垃圾回收
      }
      entry = tarArchiveInputStream.getNextTarEntry
    }
    inputStream.close()
    tarArchiveInputStream.close()
  }
}

/**
 * CREATE TABLE IF NOT EXISTS acc.ods_acc_cardSort_15min (
    Transaction_Date CHAR(8) COMMENT '交易日',
    Transaction_Time_Id CHAR(3) COMMENT '时间片ID',
    Process_Date CHAR(8) COMMENT '处理日',
    Process_Time_Id CHAR(3) COMMENT '交易时间片ID',
    Card_Type CHAR(2) COMMENT '卡类型',
    Entry_Address CHAR(6) COMMENT '进站站点',
    Exit_Address CHAR(6) COMMENT '出站站点',
    Line_Id CHAR(3) COMMENT '线路编号',
    Pasgr_Type CHAR(1) COMMENT '客流类型 0：本线客流 1：换出客流 2：换入客流 3：途经客流',
    Transaction_Cnt Float COMMENT '交易次数',
    Card_Issuer CHAR(4) COMMENT '发卡方标识'
)COMMENT 'tb清分表15min'
PARTITIONED BY (partition_month string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
location '/warehouse/acc/ods/ods_acc_cardSort_15min'
TBLPROPERTIES ('orc.compress'='SNAPPY');
 */
