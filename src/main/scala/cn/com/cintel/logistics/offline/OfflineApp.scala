package cn.com.cintel.logistics.offline


import cn.com.cintel.logistics.common.{Configuration, DateHelper, Tools}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, date_format}
/**
 * 根据不同的主题开发定义抽象方法
 * 1）数据读取
 * 2）数据处理
 * 3）数据保存
 */
trait OfflineApp {
  /**
   * 读取kudu表的数据
   * @param sparkSession
   * @param tableName
   * @param isLoadFullData
   */

  def getKuduSource(sparkSession: SparkSession, tableName:String, isLoadFullData:Boolean = false)= {
    if (isLoadFullData) {
      //加载全部的数据
      sparkSession.read.format(Configuration.SPARK_KUDU_FORMAT).options(
        Map(
          "kudu.master" -> Configuration.kuduRpcAddress,
          "kudu.table" -> tableName,
          "kudu.socketReadTimeoutMs"-> "60000")
      ).load().toDF()
    } else {
      //加载增量数据
      sparkSession.read.format(Configuration.SPARK_KUDU_FORMAT).options(
        Map(
          "kudu.master" -> Configuration.kuduRpcAddress,
          "kudu.table" -> tableName,
          "kudu.socketReadTimeoutMs"-> "60000")
      ).load()
        .where(date_format(col("cdt"), "yyyyMMdd") === DateHelper.getyesterday("yyyyMMdd")).toDF()
    }
  }
  /**
   * 数据处理
   * @param sparkSession
   */
  def execute(sparkSession: SparkSession)
  /**
   * 数据存储
   * dwd及dws层的数据都是需要写入到kudu数据库中，写入逻辑相同
   * @param dataFrame
   * @param isAutoCreateTable
   */
  def save(dataFrame:DataFrame, tableName:String, isAutoCreateTable:Boolean = true): Unit = {
    //允许自动创建表
    if (isAutoCreateTable) {
      Tools.autoCreateKuduTable(tableName, dataFrame)
    }
    //将数据写入到kudu中
    dataFrame.write.format(Configuration.SPARK_KUDU_FORMAT).options(Map(
      "kudu.master" -> Configuration.kuduRpcAddress,
      "kudu.table" -> tableName
    )).mode(SaveMode.Append).save()
  } }
