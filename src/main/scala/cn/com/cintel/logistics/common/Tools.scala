package cn.com.cintel.logistics.common

import java.util
import org.apache.commons.lang3.StringUtils
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{Metadata, StructField, StructType}
/**
 * kudu操作的工具类
 */
object Tools {
  /**
   * 创建kudu表
   */
  def autoCreateKuduTable(tableName:String, dataFrame: DataFrame, primaryField:String = "id",
                          showSchema:Boolean = false)={
    /**
     * 创建kudu表的约束？
     * kudu表中必须要有一个主键列，更新数据和删除数据都需要根据主键更新，应该使用哪个列作为主键？id
     * kudu表的字段有哪些？oracle和mysql的表有哪些字段，kudu表就有哪些字段
     * kudu表的名字是什么？使用数据中传递的表明作为kudu的表名
     */
    /**
     * 实现步骤：
     * 1）获取kudu的上下文对象：KuduContext
     * 2）判断kudu中是否存在这张表，如果不存在则创建
     * 3）判断是否指定了主键列
     * 4）生成kudu表的结构信息
     * 5）创建表
     */
    //1）获取kudu的上下文对象：KuduContext
    val kuduContext: KuduContext = new KuduContext(Configuration.kuduRpcAddress,
      dataFrame.sqlContext.sparkContext)
    //2）判断kudu中是否存在这张表，如果不存在则创建

    if(!kuduContext.tableExists(tableName)){
      //创建表
      //3）判断是否指定了主键列
      if(StringUtils.isEmpty(primaryField)){
        println(s"没有为${tableName}指定主键字段，将使用默认【id】作为主键列,如果表中存在该字段则创建成功,否则抛出异常退出程序!")
 }
          //如果打印schema
          if(showSchema) {
          println("=========原始数据中的schema==============")
          /**
           * root
           * |-- citycode: string (nullable = true)
           * |-- id: long (nullable = true)
           * |-- lat: double (nullable = true)
           * |-- level: string (nullable = true)
           * |-- lng: double (nullable = true)
           * |-- mername: string (nullable = true)
           * |-- name: string (nullable = true)
           * |-- opType: string (nullable = true)
           * |-- pid: long (nullable = true)
           * |-- pinyin: string (nullable = true)
           * |-- sname: string (nullable = true)
           * |-- yzcode: string (nullable = true)
           */
          dataFrame.printSchema()
          }
          //4）生成kudu表的结构信息（使用dataframe的schema作为kudu表的字段信息）
          //在kudu中主键列是不能为空的， 但是schema信息中的所有列都是可以为空的， 所以需要将主键列设置为非空类型
          val schema: StructType = new StructType(dataFrame.schema.map(field => {
          StructField.apply(field.name, field.dataType, {
          //判断当前的列名是否是主键列，如果是主键列，则列不能允许为空
          if (primaryField == field.name) false else true
          }, Metadata.empty)
          }).toArray)
          if(showSchema) {
          println("=========kudu表设置主键列以后的schema==============")
          schema.printTreeString()
          }
          //指定主键列
          val primaryFieldName: String = dataFrame.schema.apply(primaryField).name
          val createTableOptions = new CreateTableOptions()
          //指定副本数
          createTableOptions.setNumReplicas(1)
          //指定分区方式
          createTableOptions.addHashPartitions(util.Arrays.asList(primaryFieldName), 3)
          //创建表
          kuduContext.createTable(tableName, schema, Array(primaryFieldName), createTableOptions)
          }else{
          println(s"${tableName}表已经存在，无需创建！")
      }
    } }