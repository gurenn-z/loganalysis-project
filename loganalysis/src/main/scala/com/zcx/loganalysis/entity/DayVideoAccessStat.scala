package com.zcx.loganalysis.entity

/**
  * 每天课程访问次数实体类,与 MySQL中的 day_video_access_topn_stat 表对应
  * @param day
  * @param cmsId
  * @param times
  */
case class DayVideoAccessStat(day: String, cmsId: Long, times: Long)