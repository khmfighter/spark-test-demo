
package com.test.lib.k_means

import org.apache.commons.math3.ml.distance.EuclideanDistance
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.Spark_Log


object K_Means {
  def main(args: Array[String]): Unit = {

    val a = new K_Means()
    a.mainmethod()
  }
}

class K_Means() extends Serializable {

  def mainmethod() = {
    Spark_Log() //config("spark.sql.warehouse.dir","file:///")
    val conf = new SparkConf().setAppName("KMeansExample2").setMaster("local[1]").set("spark.sql.warehouse.dir", "file:///")
    val sc = new SparkContext(conf)

    // Load and parse the data
    val data = sc.textFile("data/mllib/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 4 //最大分类数
    val numIterations = 20 //迭代次数
    val clusters = KMeans.train(parsedData, numClusters, numIterations) //训练模型
    //计算中心点
    clusters.clusterCenters.foreach(println)
    //随机取一个点
    val p_i = Vectors.dense(0.0, 0.0,0.1)
    // 轮廓系数 [-1,1] 越大越好
    val si = SilhouetteCoefficient(p_i,clusters,parsedData)
    println("轮廓系数=  "+si)

    //option2
    // 欧几里得距离 它越小，说明聚类越好
    val WSSSE = clusters.computeCost(parsedData)
    println("欧几里得距离= " + WSSSE) //平方误差的和
    sc.stop()

  }
  // 轮廓系数
  def SilhouetteCoefficient(p_i : Vector,clusters:KMeansModel,parsedData:RDD[Vector]):Double ={
    //预测该点
    val prodicti = clusters.predict(p_i).toInt
    println("prodicti " + prodicti)

    //计算ge簇 平均距离
    val dtInPoint_k = parsedData.map(k_Dis_all(p_i, _, clusters)).cache()
    val count = dtInPoint_k.map(a => (a._1, a._2)).reduceByKey(_ + _)
    val reduce = dtInPoint_k.map(a => (a._1, a._3)).reduceByKey(_ + _)

    //(0.0,(5.0,5.43840620433566))
    //(1.0,(1.0,12.727922061357855))
    val avg = count.join(reduce).map(mapavg).collect()
    //val vectoarr= avg.toArray
    println("all : " + avg.length)

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
    println("a(i),b*(i)的最大值是："+maxa_b)

    val s_i = (outavg - intavg) / maxa_b
    //println("轮廓系数" + s_i)
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
}

