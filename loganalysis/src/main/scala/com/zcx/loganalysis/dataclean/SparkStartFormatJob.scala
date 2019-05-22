package com.zcx.loganalysis.dataclean

import org.apache.spark.sql.SparkSession

/**
  * 第一步清洗：抽取出我们所需要的指定列的数据
  */
object SparkStartFormatJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkStartFormatJob")
      .master("local[2]")
      .getOrCreate()

    val access = spark.sparkContext.textFile("F:/MyProject/data/sparksql/access.log")
    access.take(10).foreach(println)

    access.map(line => {
      val split = line.split(" ")
      val ip = split(0)
      /**
        * 原始日志的第三个和第四个字段拼接起来就是完整的访问时间
        */
      val time = split(3) + " " + split(4)
    }).take(10).foreach(println)

    spark.stop()
  }

}
