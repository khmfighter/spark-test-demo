package cn.nuecloud.bigdata.dasuan.analysis.fpgrowth

import cn.nuecloud.bigdata.utils.{DSLogging}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.FPGrowthModel
import org.apache.spark.rdd.RDD

/**
  * 频繁项集挖掘类
  * @author xuhaifeng
  * @version Neucloud2016 2016-11-18
  */
class FPGrowth(@transient sc:SparkContext) extends DSLogging{

  /**
    * 训练模型
    * @param minSupport 最小支持度
    * @param numPartitions partition个数
    * @return
    */
  def fpGrowth(dataSource:Any,numPartitions:Int,minSupport:Double=0.3):FPGrowthModel[String] = {
    @transient
    var data:RDD[Array[String]] = null
    dataSource match {
      case dataSource:RDD[Array[String]] =>data = dataSource
      case dataSource:String=>data=sc.textFile(dataSource).map(s => s.trim.split(' '))
    }

    val model = new org.apache.spark.mllib.fpm.FPGrowth().setMinSupport(minSupport).setNumPartitions(numPartitions)

    model.run(data)
  }

  /**
    * 打印符合最小支持度的频繁项集
    * @param model FPG模型
    */
  def printFreqItem(model:FPGrowthModel[String]):Unit = {
    model.freqItemsets.collect().foreach { itemSet =>
      println(itemSet.items.mkString("[", ",", "]") + ", " + itemSet.freq)
    }
  }

  /**
    * 打印符合最小支持度和最小置信度的关联项
    * @param model FPG模型
    * @param minConfidence 最小置信度
    */
  def printGenerateAssociation(model:FPGrowthModel[String],minConfidence:Double):Unit = {
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent.mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
  }
}
