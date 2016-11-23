package cn.nuecloud.bigdata.dasuan.analysis.correlation

import cn.nuecloud.bigdata.utils.{SparkContextImpl, Spark_Log}
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by JaryZhen on 2016/8/30 0030.
  */
object CorRelations {

  Spark_Log()
  def transs(row: Row): Double = {
    val swq = row.toString()
    swq.substring(1,swq.length-1).toDouble
  }

  def relationDouble(Tx :DataFrame, Ty:DataFrame): Double ={
    relationDouble(Tx,Ty,"pearson")
  }
  def relationDouble(Tx :DataFrame, Ty:DataFrame,method : String) : Double = {
//    val xData = Array(1.0, 0.0, -2.0)
//    val yData = Array(4.0, 5.0, 3.0)
    method match {
      case "pearson" =>
      case "spearman" =>
      case _ => throw new IllegalArgumentException("spearman or pearson")
    }
    val seriesX: RDD[Double] = Tx.map(transs(_)) // a series

    val seriesY: RDD[Double] = Ty.map(transs(_)) // must have the same number of partitions and cardinality as seriesX


    val correlation: Double = Statistics.corr(seriesX, seriesY, method)
    //println(correlation)
    val re = f"${correlation}%1.3f"
    return re.toDouble
  }

  def main(args: Array[String]): Unit = {
    val xData = Array(1.0, 0.0, -2.0)
    val yData = Array(4.0, 5.0, 3.0)

    //val a = makeCorrelationDouble(xData,yData)
    val data = Seq(
      Vectors.dense(1.0,1.0),
      Vectors.dense(4.0,1.0)
//      Vectors.dense(6.0, 7.0, 0.0, 8.0),
//      Vectors.dense(9.0, 0.0, 0.0, 1.0)
    )

    //println(a)
  }
}

