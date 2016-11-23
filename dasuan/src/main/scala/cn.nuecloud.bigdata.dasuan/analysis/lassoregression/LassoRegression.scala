package cn.nuecloud.bigdata.dasuan.analysis.lassoregression

import cn.nuecloud.bigdata.utils.DSLogging
import org.apache.spark.mllib.regression.{LassoWithSGD, LassoModel, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

/**
  * Lasso Regression
  * @author xuhaifeng
  * @version Neucloud2016 2016-10-25
  */

class LassoRegression() extends DSLogging{


  /**
    * lasso 模型训练
    * @param dataSource 数据源
    * @param numIterations 迭代次数
    * @param stepSize  迭代步长
    * @param regParam 正则化参数
    * @param miniBatchFraction 每次迭代参与的样本比例
    * @return
    */
  def lasso(dataSource:RDD[LabeledPoint],numIterations:Int=100,stepSize:Double=1.0,regParam:Double=0.01,miniBatchFraction:Double=1.0):LassoModel = {
    LassoWithSGD.train(dataSource,numIterations,stepSize,regParam,miniBatchFraction)
  }

  /**
    * 单点预测
    * @param model lasso model
    * @param testData 待预测数据
    * @return
    */
  def predict(model:LassoModel,testData:Vector):Double = {
    model.predict(testData)
  }

  /**
    * 多点预测
    * @param model lasso model
    * @param testData 待预测数据
    * @return
    */
  def predict(model:LassoModel,testData:RDD[Vector]):RDD[Double] = {
    model.predict(testData)
  }

  /**
    *
    * @param model
    */
  def printWeights(model:LassoModel):Unit = {
    val weights = model.weights
    for(i <- 0 until weights.size){
      println(weights(i))
    }
  }
 }