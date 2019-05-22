package com.zcx.loganalysis.log

import com.zcx.loganalysis.utils.AccessConvertUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 使用Spark完成数据清洗操作
  */
object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()
    // 读取数据
    val accessRDD = spark.sparkContext.textFile("F:/MyProject/data/sparksql/access.log")
//    accessRDD.take(10).foreach(println)

    // RDD ==> DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)), AccessConvertUtil.struct)
//    accessDF.printSchema()
//    accessDF.show(false)

    accessDF.write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save("F:/MyProject/data/loganalysis/clean")

    spark.stop()
  }

}
