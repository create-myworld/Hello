import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 基于Scala语言使用SparkCore编程实现词频统计：WordCount
 * 从HDFS上读取数据，统计WordCount，将结果保存到HDFS上
 */
object SparkWordCount {
  // TODO: 当应用运行在集群上的时候，MAIN函数就是Driver Program，必须创建SparkContext对象
  def main(args: Array[String]): Unit = {
    // 创建SparkConf对象，设置应用的配置信息，比如应用名称和应用运行模式
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkWordCount")
    // TODO: 构建SparkContext上下文实例对象，读取数据和调度Job执行
    val sc: SparkContext = new SparkContext(sparkConf)



    // val sqlContext = new HiveContext(sc)
    // 第一步、读取数据，封装到RDD集合，认为列表List
    // val inputRDD: RDD[String] = sc.textFile("src/data/wordcount.data")
    // 第二步、处理数据，调用RDD中函数，认为调用列表中的函数
    //inputRDD.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)
    /***
     *

    // a. 每行数据分割为单词
    val wordsRDD = inputRDD.flatMap(line => line.split("\\s+"))
    // b. 转换为二元组，表示每个单词出现一次
    val tuplesRDD: RDD[(String, Int)] = wordsRDD.map(word => (word, 1))
    // c. 按照Key分组聚合
    val wordCountsRDD: RDD[(String, Int)] = tuplesRDD.reduceByKey((tmp, item) => tmp + item)
    // 第三步、输出数据
    wordCountsRDD.foreach(println)
    // 保存到为存储系统，比如HDFS
    wordCountsRDD.saveAsTextFile(s"src/data/swc-output-${System.currentTimeMillis()}")
    // 为了测试，线程休眠，查看WEB UI界面
    Thread.sleep(10000000)
    // TODO：应用程序运行接收，关闭资源

     */
    //todo-1:根据数据直接创建DF
 /*   val arr: Array[String] = sc.textFile("src/data/hello").collect()
    val arr1: Array[String] = arr.slice(1, arr.length)
    val rdd: RDD[String] = sc.parallelize(arr1)
    //
    val schema = StructType(arr(0).split(",").map(
      filedName => StructField(filedName, StringType, true)
    ))

    val rowRDD = rdd.map(_.split(",")).map(p => Row(p: _*))
*/


    sc.stop()
  } }
