package com.zcx.loganalysis.log

import com.zcx.loganalysis.dao.StatDAO
import com.zcx.loganalysis.entity.{DayCityVideoAccessStat, DayVideoAccessStat, DayVideoTrafficsStat}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * TopN 统计 Spark 作业
  */
object TopNStatJob {

  /**
    * 最受欢迎的 TopN 课程
    *
    * @param spark
    * @param accessDF
    * @return
    */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
    // DataFrame 方式统计
    //    import spark.implicits._
    //
    //    val videoAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
    //      .groupBy("day", "cmsId")
    //      .agg(count("cmsId").as("times"))
    //      .orderBy($"times".desc)
    //
    //    videoAccessTopNDF.show(false)


    // sql 方式统计
    // 建立临时表
    accessDF.createOrReplaceTempView("access_logs")
    val videoAccessTopNDF = spark.sql("select day, cmsId, count(1) as times from access_logs " +
      " where day = '20170511'" +
      " and cmsType='video' " +
      " group by day, cmsId order by times desc")

    videoAccessTopNDF.show(false)


    /**
      * 将统计结果写入到 MySQL 中
      */
    try
      videoAccessTopNDF.foreachPartition(records => {

        val list = new ListBuffer[DayVideoAccessStat]

        records.foreach(info => {

          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          // 不建议在此处进行数据库的数据插入，而是先构建list，再通过批量的方式插入MySQL
          list.append(DayVideoAccessStat(day, cmsId, times))
        })

        // 批量插入
        StatDAO.insertDayVideoAccessTopN(list)

      })

    catch {
      case e: Exception => e.printStackTrace()
    }

  }

  /**
    * 按照地市进行统计TopN课程
    *
    * @param spark
    * @param accessDF
    * @return
    */
  def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {

    import spark.implicits._

    val cityAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("city", "day", "cmsId")
      .agg(count("cmsId").as("times"))

    //    cityAccessTopNDF.show(false)

    // Window 函数在 Spark SQL的使用
    // 窗口函数 row_number 的作用是根据表中字段进行分组，然后根据表中的字段排序，给组中的每条记录添
    //     加一个序号；且每组的序号都是从1开始，可利用它的这个特性进行分组取top-n
    val top3DF = cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),

      cityAccessTopNDF("times"),
      // 根据 city 分组，根据 times 降序排序
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
        .orderBy(cityAccessTopNDF("times").desc))
        .as("times_rank")

    ).filter("times_rank <= 3") //.show(100, false)

    /**
      * 将统计结果写入到 MySQL 中
      */
    try {
      top3DF.foreachPartition(records => {
        val list = new ListBuffer[DayCityVideoAccessStat]

        records.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")

          list.append(DayCityVideoAccessStat(day, cmsId, city, times, timesRank))
        })

        StatDAO.insertDayCityVideoAccessTopN(list)

      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 按照流量进行统计
    */
  def videoTrafficsTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {

    import spark.implicits._

    val cityAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "cmsId")
      .agg(sum("traffic").as("traffics"))
      .orderBy($"traffics".desc) //.show(false)

    /**
      * 将统计结果写入到MySQL中
      */
    try {
      cityAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoTrafficsStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")
          list.append(DayVideoTrafficsStat(day, cmsId, traffics))
        })

        StatDAO.insertDayVideoTrafficsAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkStatCleanJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]")
      .getOrCreate()

    val accessDF = spark.read.format("parquet").load("F:/MyProject/data/loganalysis/clean")
    //    accessDF.printSchema()
    //    accessDF.show(false)

    val day = "20170511"

    StatDAO.deleteData(day)

    // 最受欢迎的 TopN 课程
    videoAccessTopNStat(spark, accessDF, day)

    // 按照地市进行统计TopN课程
    cityAccessTopNStat(spark, accessDF, day)

    // 按照流量进行统计
    videoTrafficsTopNStat(spark, accessDF, day)

    spark.stop()
  }

}
