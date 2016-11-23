package cn.nuecloud.bigdata.dasuan.analysis.svm

import cn.nuecloud.bigdata.utils.{DSLogging, SparkContextImpl, Spark_Log}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by JaryZhen on 2016/11/22 0022.
  */
object SVM_Scala extends DSLogging{
  Spark_Log()
  def model(@transient sc : SparkContext, source : Any): Unit ={

    @transient
    var rddData : RDD[LabeledPoint] = null
    source match {
      case s1 : DataFrame => rddData = ???
      case s2 : String => {rddData = MLUtils.loadLibSVMFile(sc, s2)}
    }
    //val rddData = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

    // Split rddData into training (60%) and test (40%).
    val splits = rddData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    // Save and load model
    // model.save(sc, "myModelPath")
    //val sameModel = SVMModel.load(sc, "myModelPath")
    sc.stop()
  }
  def main(args: Array[String]): Unit = {

    val sc = SparkContextImpl("svm l2")
    val source  = "data/mllib/sample_libsvm_data.txt"
    this.model(sc,source)

  }
}
