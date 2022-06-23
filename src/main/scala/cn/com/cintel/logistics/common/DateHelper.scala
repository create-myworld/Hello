package cn.com.cintel.logistics.common

import java.text.SimpleDateFormat
import java.util.Date
/**
 * 时间处理工具类
 */
object DateHelper {
  /**
   * 返回昨天的时间
   */
  def getyesterday(format:String)={
    //当前时间减去一天（昨天时间）
    new SimpleDateFormat(format).format(new Date(System.currentTimeMillis() - 1000 * 60 * 60 * 24))
  }
  /**
   * 返回今天的时间
   * @param format
   */
  def gettoday(format:String) = {
    //获取指定格式的当前时间
    new SimpleDateFormat(format).format(new Date)
  } }
