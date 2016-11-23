package cn.nuecloud.bigdata.dasuan.analysis.bisectingkmeans

import cn.nuecloud.bigdata.utils.{DSLogging}
import org.apache.spark.mllib.clustering.BisectingKMeansModel
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame}

/**
  * 二分KMeans
  *
  * @author xuhaifeng
  * @version Neucloud2016 2016-10-24
  */
class BisectingKMeans() extends DSLogging {

  def cluster(sourceData: DataFrame,k:Int=4,numIterations:Int=20,minDivisibleClusterSize:Double=1):BisectingKMeansModel = {
    val parsedData = dataTranslate(sourceData)
   new org.apache.spark.mllib.clustering.BisectingKMeans().setK(k).setMaxIterations(numIterations)
     .setMinDivisibleClusterSize(minDivisibleClusterSize).run(parsedData)
  }

  def predict(model:BisectingKMeansModel,points:Vector):Int = {
    model.predict(points)
  }

  def computeCost(model:BisectingKMeansModel,sourceData: DataFrame):Double={
    val parsedData = dataTranslate(sourceData)
    model.computeCost(parsedData)
  }

  private def dataTranslate(sourceData: DataFrame):RDD[Vector] = {
    val parsedData = sourceData.rdd.map(row=>Vectors.dense(row.toString().substring(1,row.toString().size-1).split(",").map(x=>x.toDouble)))
    parsedData
  }

}
