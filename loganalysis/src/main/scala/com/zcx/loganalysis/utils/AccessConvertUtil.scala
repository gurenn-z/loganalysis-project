package com.zcx.loganalysis.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 访问日志转换（输入 ==> 输出）工具类
  */
object AccessConvertUtil {

  // 定义的输出的字段
  val struct = StructType {
    Array(
      // url
      StructField("url", StringType),
      // video or article
      StructField("cmsType", StringType),
      // cms 编号
      StructField("cmsId", LongType),
      // 流量
      StructField("traffic", LongType),
      // ip信息
      StructField("ip", StringType),
      // 城市信息
      StructField("city", StringType),
      // 访问时间
      StructField("time", StringType),
      // 分区字段
      StructField("day", StringType)
    )
  }

  /**
    * 根据输入的每一行信息换装成输出的样式
    * @param log 输入的每一行记录信息
    */
  def parseLog(log: String) = {

    try {
      val splits = log.split("\t")

      val url = splits(1)
      val traffic = splits(2).toLong
      val ip = splits(3)

      // http://www.imooc.com/code/547   ===>  code/547  547
      val domain = "http://www.imooc.com/"
      val cms = url.substring(url.indexOf(domain) + domain.length)
      val cmsTypeId = cms.split("/")

      var cmsType = ""
      var cmsId = 0l
      if (cmsTypeId.length > 1) {
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }

      val city = IpUtils.getCity(ip)
      val time = splits(0)
      val day = time.substring(0, 10).replaceAll("-", "")

      // 这个 row 里面的字段要和 struct 中的字段对应上
      Row(url, cmsType, cmsId, traffic, ip, city, time, day)

    } catch {
      case e: Exception => Row(0)

    }
  }


}
