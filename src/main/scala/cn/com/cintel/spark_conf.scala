package cn.com.cintel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object spark_conf {

  def main(args: Array[String]): Unit = {

    //1）初始化sparkConf对象

    //2）创建sparkSession对象

    val session: SparkSession = SparkSession.builder().master("local")
      .appName("wordcount")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // 将DF转换成DS
    // textFile方法返回dataset csv方法返回dataFrame
    val frame: Dataset[String] = session.read.textFile("D:\\IdeaProjects\\Spark_learn\\src\\data\\wordcount.data")

    // frame.show()
    // spark隐式转换
    import session.implicits._
    // TODO-1 将dataset转换成rdd 进行wordCount
    /*val rdd: RDD[String] = frame.rdd
    val value = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    value.collect().map(println)*/
   // TODO-2 将dataset拉平后创建临时视图
    frame.flatMap(_.split(" ")).createTempView("tmp")

    session.sql("select value,count(*) cnt from tmp group by value order by count(*) desc").show()

    session.stop()
    //数据处理

}

}



/*
import cn.com.cintel.logistics.common.{CodeTypeMapping, Configuration, OfflineTableDefine, SparkUtils}
import cn.itcast.logistics.offline.OfflineApp
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
/**
 * 快递单主题开发
 * 将快递单事实表的数据与相关维度表的数据进行关联，然后将拉宽后的数据写入到快递单宽表中
 * 采用DSL语义实现离线计算程序
 *
 * 最终离线程序需要部署到服务器，每天定时执行（azkaban定时调度）
 */
object ExpressBillDWD extends OfflineApp {
  //定义应用的名称
  val appName = this.getClass.getSimpleName
  /**
   * 入口函数
   * @param args
   */
  def main(args: Array[String]): Unit = {
    /**
     * 实现步骤：
     * 1）初始化sparkConf对象
     * 2）创建sparkSession对象
     * 3）加载kudu中的事实表和维度表的数据（将加载后的数据进行缓存）
     * 4）定义维度表与事实表的关联
     * 5）将拉宽后的数据再次写回到kudu数据库中（DWD明细层）
     * 5.1：创建快递单明细宽表的schema表结构
     * 5.2：创建快递单宽表（判断宽表是否存在，如果不存在则创建）
     * 5.3：将数据写入到kudu中
     * 6)：将缓存的数据删除掉
     */
    //1）初始化sparkConf对象
    val sparkConf: SparkConf = SparkUtils.autoSettingEnv(
      SparkUtils.sparkConf(appName)
    )
    //2）创建sparkSession对象
    val sparkSession: SparkSession = SparkUtils.getSparkSession(sparkConf)
    sparkSession.sparkContext.setLogLevel(Configuration.LOG_OFF)
    //数据处理
    execute(sparkSession)
  }
  /**
   * 数据处理
   *
   * @param sparkSession
   */
  override def execute(sparkSession: SparkSession): Unit = {
    sparkSession.stop()
  } }*/
