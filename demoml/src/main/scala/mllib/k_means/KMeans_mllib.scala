
package mllib.k_means

import java.util

import ml.K_Means
import org.apache.commons.math3.ml.distance.EuclideanDistance
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.Spark_Log


object K_Means extends Serializable{

  def main(args: Array[String]): Unit = {

    val datadir: String = "data/mllib/kmeans_data.txt"
    val k = 3  //最大分类数
    val numIterations = 20  //迭代次数
    val a = new K_Means(datadir)
    val clust = a.clusterCenters(k, numIterations)

    val al = a.SCall(clust)
    println("轮廓系数" + al)
    a.stop()
  }
}

class K_Means(datadir: String) extends Serializable {

  //.set("spark.driver.allowMultipleContexts","true")
  @transient
  val sc: SparkContext = {
    Spark_Log() //config("spark.sql.warehouse.dir","file:///")
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[10]").set("spark.sql.warehouse.dir", "file:///")
    val sc = new SparkContext(conf)
    sc
  }
  @transient
  var parsedData : RDD[Vector] ={
    val data = sc.textFile(datadir)
    parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    parsedData
  }

  def clusterCenters(k:Int,numIterations:Int) : KMeansModel= {
    // Load and parse the data
    // Cluster the data into two classes using KMeans
    val clusters = KMeans.train(parsedData, k, numIterations) //训练模型
    //计算中心点
    //clusters.clusterCenters.foreach(println)
    clusters
  }
  def printCenters (clusters:KMeansModel): util.ArrayList[String] = {
    val list = new util.ArrayList[String]()
    clusters.clusterCenters.foreach(toList)
    def toList(v:Vector)= {
      list.add(v.toString)
      //println(v)
    }
    list
  }
  // 欧几里得距离 它越小，说明聚类越好
  def ouj(cluster:KMeansModel):Double = {
    val WSSSE:Double = cluster.computeCost(parsedData)
    //println("欧几里得距离= " + WSSSE) //平方误差的和
    WSSSE
  }

  def SCall(clusters:KMeansModel): Double ={
    var all = 0.0
    val count = parsedData.count().toInt
    val sample = parsedData.take(Int.MaxValue)
    for (i <- sample){
      if (i !=null){
        val singr = SCSingle(i, clusters)
        all += singr
      }
    }
    all/count
  }
  // 单点轮廓系数 // 轮廓系数 [-1,1] 越大越好
  def  SCSingle(p_i : Vector, clusters:KMeansModel):Double ={
    //预测该点
    val prodicti = clusters.predict(p_i)
    println(p_i+" prodicti is " + prodicti)

    //计算各簇 平均距离
    val dtInPoint_k = parsedData.map(k_Dis_all(p_i, _, clusters)).cache()
    val count = dtInPoint_k.map(a => (a._1, a._2)).reduceByKey(_ + _)
    val reduce = dtInPoint_k.map(a => (a._1, a._3)).reduceByKey(_ + _)

    //(0.0,(5.0,5.43840620433566))
    //(1.0,(1.0,12.727922061357855))
    val avg = count.join(reduce).map(mapavg).collect()
    //val vectoarr= avg.toArray
    println("all length : " + avg.length)
    for (i <- 0 to avg.length - 1) {
      println("all : " + avg {i})
    }
    // 从各簇中找出 最小和次小 因为最小可能是簇内距离 则次小就是簇外最小
    var min = avg {0}._2.toDouble//最小值
    var minTh = avg {0}._2.toDouble //次小

    for (i <- 0 to avg.length - 1) {
      val element = avg{i}._2
      //zuixiao
      if(element < min){
        min = element;
      }
    }

    if (min == avg {0}._2.toDouble) minTh = avg {1}._2.toDouble
    for (i <- 0 to avg.length - 1) {
      val element = avg{i}._2
      if(element < minTh && element != min){
        minTh = element;
      }
    }
    println(min +"  <--最小和次小--> "+minTh)
    val avgmap =avg.toMap
    var intavg = avgmap.get(prodicti).getOrElse(0.0)

    //判断最小是不是簇内距离
    var outavg =0.0
    if(intavg==min){
      outavg = minTh
    }else{
      outavg =min
    }

    println("簇外最小距离："+outavg +" 簇内距离: "+intavg)
    //取a(i),b*(i)的最大值
    val maxa_b = {
      if(outavg < intavg){
        intavg
      }else{outavg}
    }
    println("a(i) b*(i)的最大值是："+maxa_b)
    val s_i = (outavg - intavg) / maxa_b
    println("......................"+p_i+"的轮廓系数是="+s_i)
    println("----------------------------------------------")
    println("----------------------------------------------")
    println("----------------------------------------------")

    s_i
  }
  //点到各簇中心内所有点的距离
  def k_Dis_all(p_i: Vector, data: Vector, clusters: KMeansModel): (Double, Double, Double) = {
    val pv = p_i.toArray
    val dv = data.toArray
    //println(dv)
    val pro = clusters.predict(data)
    //计算距离 d=sqrt( ∑(xi1-xi2)^ )
    val dis = new EuclideanDistance().compute(pv, dv)
    //println("oushi .. "+dis)
    println("点= " + data + " 属于簇：" + pro + " 距离i点=" + dis)
    (pro, 1, dis)
  }

  //点到各簇中心内所有点的平均距离
  def mapavg(d: (Double, (Double, Double))): (Int, Double) = {
    val avg = d._2._2 / d._2._1
    (d._1.toInt, avg)
  }
  def stop(): Unit ={
    this.sc.stop()
  }

}

