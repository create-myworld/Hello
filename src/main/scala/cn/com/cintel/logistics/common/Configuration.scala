package cn.com.cintel.logistics.common

import java.util.{Locale, ResourceBundle}
/**
 * 读取配置文件的工具类
 */
class Configuration {
  /**
   * 定义配置文件操作的对象
   */
  private val resourceBundle: ResourceBundle = ResourceBundle.getBundle("config", new Locale("zh", "CN"))
  private val sep = ":"
  // CDH-6.2.1
  val bigdataHost: String = resourceBundle.getString("bigdata.host")
  // HDFS
  val dfsUri: String = resourceBundle.getString("dfs.uri")
  // Local FS
  val localFsUri: String = resourceBundle.getString("local.fs.uri")
  // Kafka
  val kafkaBrokerHost: String = resourceBundle.getString("kafka.broker.host")
  val kafkaBrokerPort: Int = Integer.valueOf(resourceBundle.getString("kafka.broker.port"))
  val kafkaInitTopic: String = resourceBundle.getString("kafka.init.topic")
  val kafkaLogisticsTopic: String = resourceBundle.getString("kafka.logistics.topic")
  val kafkaCrmTopic: String = resourceBundle.getString("kafka.crm.topic")
  val kafkaAddress = kafkaBrokerHost+sep+kafkaBrokerPort
  // Spark
  val LOG_OFF = "OFF"
  val LOG_DEBUG = "DEBUG"
  val LOG_INFO = "INFO"
  val LOCAL_HADOOP_HOME = "E:\\softs\\hadoop-3.0.0"
  val SPARK_KAFKA_FORMAT = "kafka"
  val SPARK_KUDU_FORMAT = "kudu"
  val SPARK_ES_FORMAT = "es"
  val SPARK_CLICKHOUSE_FORMAT = "clickhouse"
  // ZooKeeper
  val zookeeperHost: String = resourceBundle.getString("zookeeper.host")
  val zookeeperPort: Int = Integer.valueOf(resourceBundle.getString("zookeeper.port"))
  // Kudu
  val kuduRpcHost: String = resourceBundle.getString("kudu.rpc.host")
  val kuduRpcPort: Int = Integer.valueOf(resourceBundle.getString("kudu.rpc.port"))
  val kuduHttpHost: String = resourceBundle.getString("kudu.http.host")
  val kuduHttpPort: Int = Integer.valueOf(resourceBundle.getString("kudu.http.port"))
  val kuduRpcAddress = kuduRpcHost+sep+kuduRpcPort

  // ClickHouse
  val clickhouseDriver: String = resourceBundle.getString("clickhouse.driver")
  val clickhouseUrl: String = resourceBundle.getString("clickhouse.url")
  val clickhouseUser: String = resourceBundle.getString("clickhouse.user")
  val clickhousePassword: String = resourceBundle.getString("clickhouse.password")
  // ElasticSearch
  val elasticsearchHost: String = resourceBundle.getString("elasticsearch.host")
  val elasticsearchRpcPort: Int = Integer.valueOf(resourceBundle.getString("elasticsearch.rpc.port"))
  val elasticsearchHttpPort: Int = Integer.valueOf(resourceBundle.getString("elasticsearch.http.port"))
  val elasticsearchAddress = elasticsearchHost+sep+elasticsearchHttpPort
  // Azkaban
  val isFirstRunnable = java.lang.Boolean.valueOf(resourceBundle.getString("app.first.runnable"))
  // ## Data path of ETL program output ##
  // # Run in the yarn mode in Linux
  val sparkAppDfsCheckpointDir = resourceBundle.getString("spark.app.dfs.checkpoint.dir")// /apps/logistics/dat-hdfs/spark-checkpoint
  val sparkAppDfsDataDir = resourceBundle.getString("spark.app.dfs.data.dir")// /apps/logistics/dat-hdfs/warehouse
  val sparkAppDfsJarsDir = resourceBundle.getString("spark.app.dfs.jars.dir")// /apps/logistics/jars
  // # Run in the local mode in Linux
  val sparkAppLocalCheckpointDir = resourceBundle.getString("spark.app.local.checkpoint.dir")// /apps/logistics/dat-local/spark-checkpoint
  val sparkAppLocalDataDir = resourceBundle.getString("spark.app.local.data.dir")// /apps/logistics/dat-local/warehouse
  val sparkAppLocalJarsDir = resourceBundle.getString("spark.app.local.jars.dir")// /apps/logistics/jars
  // # Running in the local Mode in Windows
  val sparkAppWinCheckpointDir = resourceBundle.getString("spark.app.win.checkpoint.dir") // D://apps/logistics/dat-local/spark-checkpoint
  val sparkAppWinDataDir = resourceBundle.getString("spark.app.win.data.dir")// D://apps/logistics/dat-local/warehouse
  val sparkAppWinJarsDir = resourceBundle.getString("spark.app.win.jars.dir")// D://apps/logistics/jars
  val dbOracleUrl = resourceBundle.getString("db.oracle.url")
  val dbOracleUser = resourceBundle.getString("db.oracle.user")
  val dbOraclePassword = resourceBundle.getString("db.oracle.password")
  val dbMySQLDriver = resourceBundle.getString("db.mysql.driver")
  val dbMySQLUrl = resourceBundle.getString("db.mysql.url")
  val dbMySQLUser = resourceBundle.getString("db.mysql.user")
  val dbMySQLPassword = resourceBundle.getString("db.mysql.password") }
object Configuration extends Configuration {
  def main(args: Array[String]): Unit = {
    println(Configuration.dbOracleUrl)
    println(Configuration.dbMySQLDriver)
    println(Configuration.dbMySQLUrl)
    println(Configuration.dbMySQLPassword)
    println(Configuration.bigdataHost)
  } }
