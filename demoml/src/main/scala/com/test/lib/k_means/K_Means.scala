
package com.test.lib.k_means

import org.apache.commons.math3.ml.distance.EuclideanDistance
import org.apache.spark.deploy.master.{ApplicationInfo, WorkerInfo}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import utils.Spark_Log


object K_Means{
  def main(args: Array[String]): Unit = {
    val a = new K_Means()
    a.mainmethod()
  }
}

class K_Means() extends Serializable{

  var double1: Double = 0.0

  def SilhouetteCoefficient_in(p_i: Vector,d: Vector,clusters : KMeansModel,prodicti:Int): Double={
   // println(d.size.toString)
    //val x = sc.parallelize(Seq(Vectors.dense(0.2, 0.2 ,0.2)))

    val pv = p_i.toArray
    val dv  = d.toArray

    //println(dv)
    val pro = clusters.predict(d)
    if (prodicti==pro){
      //计算距离 d=sqrt( ∑(xi1-xi2)^ )
      val dis = new EuclideanDistance().compute(pv,dv)
      //println("oushi .. "+dis)
      return dis
    }
    0.0
  }
  def SilhouetteCoefficient_out(p_i: Vector,d: Vector,clusters : KMeansModel,prodicti:Int): Double={
    // println(d.size.toString)
    //val x = sc.parallelize(Seq(Vectors.dense(0.2, 0.2 ,0.2)))

    val pv = p_i.toArray
    val dv  = d.toArray

    //println(dv)
    val pro = clusters.predict(d)
    if (prodicti!=pro){
      //计算距离 d=sqrt( ∑(xi1-xi2)^ )
      val dis = new EuclideanDistance().compute(pv,dv)
      //println("oushi .. "+dis)
      return dis
    }
    0.0
  }

  def mainmethod() = {
    Spark_Log() //config("spark.sql.warehouse.dir","file:///")
    val conf = new SparkConf().setAppName("KMeansExample2").setMaster("local[3]").set("spark.sql.warehouse.dir","file:///")
    val sc = new SparkContext(conf)

    // Load and parse the data
    val data = sc.textFile("data/mllib/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2 //最大分类数
    val numIterations = 20 //迭代次数
    val clusters = KMeans.train(parsedData, numClusters, numIterations) //训练模型
    //计算中心点
    clusters.clusterCenters.foreach(println)

    //轮廓系数
    //随机取一个点
    val p_i = Vectors.dense(0.0, 0.0)
    val prodicti = clusters.predict(p_i)
    println("prodicti "+prodicti)
    println("conut all "+parsedData.count())
    //计算簇内平均距离
    val a_i = parsedData.map(SilhouetteCoefficient_in(p_i,_,clusters,prodicti)).filter(_!=0)
    //println("簇内平均距离 "+a_i)
    val incount = a_i.count()
    val indistance = a_i.reduce(_+_)
    println("count 簇内 "+ incount)
    println("reduce "+ indistance)


    //计算簇外 平均距离
    val b_i = parsedData.map(SilhouetteCoefficient_out(p_i,_,clusters,prodicti)).filter(_!=0)
    //println("簇外平均距离 "+b_i)
    val outcount = b_i.count()
    val outdistance = b_i.reduce(_+_)
    println("count 簇外 "+outcount)
    println("reduce "+outdistance )

    val intavg = indistance/incount
    val outavg = outdistance/outcount
    val maxa_b = Math.max(intavg,outavg)
    val s_i =(outavg-intavg)/maxa_b

    println("轮廓系数"+s_i)


    //option2
    // Evaluate clustering by computing Within Set Sum of Squared Errors 通过计算平方差 来评估聚类分
    //val WSSSE = clusters.computeCost(parsedData)
    //println("Within Set Sum of Squared Errors = " + WSSSE) //平方误差的和
    sc.stop()
  }
}

