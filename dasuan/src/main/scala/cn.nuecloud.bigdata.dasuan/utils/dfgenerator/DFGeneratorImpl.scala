package cn.nuecloud.bigdata.dasuan.utils.dfgenerator

import java.util.StringTokenizer

import cn.nuecloud.bigdata.utils.{DSLogging, SparkContextImpl, Spark_Log}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors

import scala.reflect.ClassTag

/**
  * Created by Jary on 2016/10/8 0008.
  */
object DFGeneratorImpl extends DFGenerator with DSLogging {
  //setLoggingOff

  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkContextImpl("dfGenerator")
    Spark_Log()
    val datadir = "hdfs://neucloud/user/hue/test.json" //"data/testFile/test.json"
    val json = "data/mllib/pic_data.txt"
    jsonToDF(datadir, sc)
    //txtToDF(json,sc,"q,w,e").show()
    sc.stop()
  }

  override def jsonToDF(datadir: String, sc: SparkContext): DataFrame = {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json(datadir).toDF()
    sc.stop()
    df
  }

  override def txtToDF(datadir: String, sc: SparkContext, schemastring: String): DataFrame = {
    val sqlContext = new SQLContext(sc)
    val bean = sc.textFile(datadir)
    val schemaArray = schemastring.split(",")

    loginfo_a("schemaArray.length  " + schemaArray.length)
    val schma = StructType(schemaArray.map(fielname => StructField(fielname, StringType, true)))
    val rowRDD = bean.map(tokeNizer(_)).filter(_.length == schemaArray.length).map(parseToken)
    rowRDD.collect()
    val df = sqlContext.createDataFrame(rowRDD, schma)
    df
  }


  /**
    * hbase postgres mogodb 等nosql 文件
    * @param df
    * @param sc
    * @return
    */
  override def nosqlToDF[T:ClassTag](df: DataFrame, sc: SparkContext) : DataFrame = ???
  /**
    * root
    * |-- label: integer (nullable = false)
    * |-- features: vector (nullable = true)
    *
    * +-----+-------------+
    * |label|     features|
    * +-----+-------------+
    * |    0|[0.0,0.0,0.0]|
    * |    0|[0.1,0.1,0.1]|
    * |    0|[0.2,0.2,0.2]|
    */
  def dfToVector(df: DataFrame, sc: SparkContext) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    val a  = df.map(a => toArr(a.toString())).map(p => (0, Vectors.dense(p))).collect()
    val dataseq = a.toSeq
    val df2 = sqlContext.createDataFrame(dataseq).toDF("label", "features")
    df2.show
    df2
  }

  override def dfToString(df: DataFrame,sc : SparkContext): String = {
    val a  = df.map(a => toArr(a.toString()))
    a.toString()
  }

  /**
    * 将DF 保存在文件
    *
    * @param df
    * @param filepath
    */
  override def dfToFile(df: DataFrame, filepath: String,sc : SparkContext): Unit = {
    val sqlContext = new SQLContext(sc)
    val a  = df.map(a => toArr(a.toString()))
    df.write.save(filepath)
  }

  /**
    * 将DF 保存在hdfs
    *
    * @param df
    * @param fspath
    */
  override def dfToHdfs(df: DataFrame, fspath: String,sc : SparkContext): Unit = ???

  /**
    * 将DF 保存在nosql 数据库中
    *
    * @param df
    */
  override def dfToNosql(df: DataFrame,sc : SparkContext): Unit = ???

  private def tokeNizer(string: String): Array[String] = {
    val st = new StringTokenizer(string, " \t\n\n\f,", false);
    val length = st.countTokens()
    val array = new Array[String](length)
    for (i <- 0 until length if st.hasMoreElements()) {
      array(i) = st.nextElement().toString
    }
    array
  }

  private def parseToken(p: Array[String]): Row = {
    val row = Row.fromSeq(p)
    row
  }
  // 支持二维和三维
  private def toArr(s: String): Array[Double] = {
    loginfo_a(s)  //[33]
    val ss = s.substring(1,s.length-1)
    val st = new StringTokenizer(ss, " \t\n\n\f,", false);
    val length = st.countTokens()
    val array = new Array[Double](length)
    for (i <- 0 until length if st.hasMoreElements()) {
      array(i) = st.nextElement().toString.toDouble
    }
    array
  }
}
