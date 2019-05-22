package com.zcx.loganalysis.log

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TuniuPandaJob {

  def userSatisfaction(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
    // DataFrame 方式统计
        import spark.implicits._

        val userSatisNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
          .groupBy("day", "cmsId")
          .agg(count("cmsId").as("times"))
          .orderBy($"times".desc)

        userSatisNDF.show(false)




  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TuniuPandaJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]")
      .getOrCreate()

//    val accessDF = spark.read.format("parquet").load("F:/MyProject/data/loganalysis/clean")
    //    accessDF.printSchema()
    //    accessDF.show(false)


    // 最受欢迎的 TopN 课程
//    userSatisfaction(spark, accessDF)

    spark.stop()
  }
}
