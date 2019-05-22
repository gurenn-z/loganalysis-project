package com.zcx.loganalysis.test

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()

    val sc = new SparkContext(sparkConf)
    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5), 3)
    rdd.foreachPartition(partiton => {

      // partiton.size 不能执行这个方法，否则下面的foreach方法里面会没有数据，

      //因为iterator只能被执行一次

      partiton.foreach(line => {

        //save(line) 落地数据

      })


    })


    sc.stop()
  }

}
