package cn.nuecloud.bigdata.dasuan.utils.dfgenerator

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

/**
  * Created by Jary on 2016/9/20 0020.
  */
trait  DFGenerator{

  /**
    *本地文件类型 1.json
    * @param datadir
    * @param sc
    * @return DataFrame
    */
  def jsonToDF(datadir: String,sc : SparkContext): DataFrame

  /**
    * 本地文件类型 或 HDFS 2.txt，hdfs
    * @param datadir
    * @param sc
    * @param schema  eg:schema= "name,age"
    * @return DataFrame
    */
  def txtToDF(datadir: String,sc : SparkContext,schema :String): DataFrame

  /**
    * hbase postgres mogodb 等nosql 文件
    * @param df
    * @param sc
    * @return
    */
  def nosqlToDF[T: ClassTag](df : DataFrame,sc : SparkContext): DataFrame

  /**
    * 将DF 装换成 Seq , Array ,Set
    * @param df
    * @param sc
    * @return
    */

  def dfToString(df: DataFrame,sc : SparkContext): String

  /**
    * 将DF 保存在文件
    * @param df
    * @param filepath
    */
  @deprecated(" too simpal too write")
  def dfToFile(df: DataFrame, filepath: String,sc : SparkContext): Unit

  /**
    * 将DF 保存在hdfs
    * @param df
    * @param fspath
    */
  @deprecated(" too simpal too write")
  def dfToHdfs(df: DataFrame, fspath: String,sc : SparkContext): Unit

  /**
    * 将DF 保存在nosql 数据库中
    * @param df
    */
  @deprecated(" too simpal too write")
  def dfToNosql(df: DataFrame,sc : SparkContext): Unit

}
