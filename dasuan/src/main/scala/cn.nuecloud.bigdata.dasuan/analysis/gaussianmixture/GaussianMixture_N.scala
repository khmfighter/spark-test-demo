package cn.nuecloud.bigdata.dasuan.analysis.gaussianmixture

import cn.nuecloud.bigdata.utils.{DSLogging}
import org.apache.spark.mllib.clustering.GaussianMixtureModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.sql.{DataFrame}

import scala.util.Random

/**
  * 高斯混合聚类
  * @author xuhaifeng
  * @version Neucloud2016 2016-10-24
  */
class GaussianMixture_N() extends DSLogging{

  /**
    * 聚类函数
    *
    * @param k              聚类数
    * @param numIterations  迭代次数
    * @param convergenceTol 收敛误差
    * @param seed
    * @return
    */
  def cluster(sourceData:DataFrame,k:Int=2,numIterations:Int=100,convergenceTol:Double=0.01,seed:Long=new Random().nextLong())
    : GaussianMixtureModel = {
    val parsedData = dataTranslate(sourceData)
      new org.apache.spark.mllib.clustering.GaussianMixture().setK(k).setMaxIterations(numIterations)
         .setConvergenceTol(convergenceTol).setSeed(seed).run(parsedData)
  }

  /**
    * 单点预测
    *
    * @param model  高斯混合模型
    * @param points 待预测的点
    * @return 该点所在类
    */
  def predict(model:GaussianMixtureModel,points:Vector):Int = {
    model.predict(points)
  }

  /**
    * 多点预测
    *
    * @param model  高斯混合模型
    * @param points 待分类的点
    * @return 个点所在类组成的rdd
    */
  def predict(model:GaussianMixtureModel,points:RDD[Vector]): RDD[Int] = {
    model.predict(points)
  }

  /**
    * 打印各高斯函数投影的参数
    *
    * @param model  高斯混合模型
    * @param points 待分类的点
    */
  def printComponents(model:GaussianMixtureModel,points:Vector):Unit = {
    val components = model.predictSoft(points)
    for(i <- 0 until  components.length){
      println(components(i))
    }
  }

  /**
    * 打印各分类的权重
    *
    * @param model 高斯混合模型
    */
  def printWeights(model:GaussianMixtureModel):Unit = {
    val weights = model.weights
    for(i <- 0 until weights.length){
      println(weights(i))
    }
  }

  private def dataTranslate(sourceData:DataFrame):RDD[Vector] = {
    val parsedData = sourceData.rdd.map(row=>Vectors.dense(row.toString().substring(1,row.toString().size-1).split(",").map(x=>x.toDouble)))
    parsedData
  }

}

