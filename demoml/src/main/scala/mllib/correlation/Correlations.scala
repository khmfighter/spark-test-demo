package mllib.correlation

import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.Spark_Log


/**
  * Created by Administrator on 2016/8/30 0030.
  */
object Correlations {

  Spark_Log()

  def makeCorrelationDouble(Tx:Array[Double] , Ty:Array[Double]) : Double = {
    val conf = new SparkConf().setAppName("Correlations").setMaster("local[1]").set("spark.sql.warehouse.dir","file:///")
    val sc = new SparkContext(conf)

//    val xData = Array(1.0, 0.0, -2.0)
//    val yData = Array(4.0, 5.0, 3.0)

    val x = sc.parallelize(Tx)
    val y = sc.parallelize(Ty)

    val seriesX: RDD[Double] = x // a series
    val seriesY: RDD[Double] = y // must have the same number of partitions and cardinality as seriesX

    // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a
    // method is not specified, Pearson's method will be used by default.
    val correlation: Double = Statistics.corr(seriesX, seriesY, "spearman")
    //println(correlation)

    sc.stop()

    return correlation
  }
  def makeCorrelationVector(Tx:Seq[Vector] ) : Matrix = {
    val conf = new SparkConf().setAppName("Correlations").setMaster("local[1]").set("spark.sql.warehouse.dir","file:///")
    val sc = new SparkContext(conf)

    val x = sc.parallelize(Tx)
   // val y = sc.parallelize(Ty)
    //println(correlation)

    val data: RDD[Vector] = x // note that each Vector is a row and not a column

    // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method.
    // If a method is not specified, Pearson's method will be used by default.
     val correlMatrix: Matrix = Statistics.corr(data, "spearman")
    sc.stop()

    return correlMatrix
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
    val a = makeCorrelationVector(data)
    println(a)
  }
}

