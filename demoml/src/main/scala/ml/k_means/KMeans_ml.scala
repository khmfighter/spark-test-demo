package ml.k_means

import java.util

import org.apache.commons.math3.ml.distance.EuclideanDistance
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.Spark_Log


object KMeans_ml {
  //System.setProperty("hadoop.home.dir", "C:\\Program Files (x86)\\hadoop-common-2.2.0-bin-master\\bin");
  Spark_Log()
  def main(args: Array[String]): Unit = {
    //mllib/sample_kmeans_data.txt
    val datadir: String = "data/mllib/sample_kmeans_data.txt"
    val k = 3  //最大分类数
    val numIterations = 20  //迭代次数
    val a = new K_Means(datadir)
    val clust = a.clusterCenters(k, numIterations)
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
  val sqlContext: SQLContext = {
    val sqlContext = new SQLContext(sc)
    sqlContext
  }
  @transient
  var parsedData :DataFrame  ={
    //val df = sqlContext.read.json("examples/src/main/resources/people.json")
    val parsedData: DataFrame = sqlContext.createDataFrame(Seq(
      (1, Vectors.dense(0.0, 0.0, 0.0)),
      (2, Vectors.dense(0.1, 0.1, 0.1)),
      (3, Vectors.dense(0.2, 0.2, 0.2)),
      (4, Vectors.dense(9.0, 9.0, 9.0)),
      (5, Vectors.dense(9.1, 9.1, 9.1)),
      (6, Vectors.dense(9.2, 9.2, 9.2))
    )).toDF("label", "features")
    parsedData
  }
  def clusterCenters(k:Int,numIterations:Int) : KMeansModel= {

    // Cluster the data into two classes using KMeansML
    val kmeans = new KMeans().setK(k).setFeaturesCol("features")
      .setPredictionCol("prediction")
    //训练模型
    val clusters = kmeans.fit(parsedData)
   // val v = Vectors.dense(1,1,1)

    println("xish "+SCall(clusters))
    //clustersDF.filter("features=[0.0,0.0,0.0]").selectExpr("features").show()

    clusters
  }

  // 单点轮廓系数 // 轮廓系数 [-1,1] 越大越好
  def SCall(clusters:KMeansModel): Double ={
    var all = 0.0
    val clustersDF = clusters.transform(parsedData).select("*").orderBy("label").cache()
    val count = clustersDF.count().toInt
    println(count)
    val sample = clustersDF.take({
     if (Int.MaxValue>count)
       count
      else  Int.MaxValue})
    for (i <- sample){
      if (i !=null){
        val singr = SCSingle(i, clusters)
        all += singr
      }
    }
    all/count
  }
  //预测
  def predict(r : Row, clusters:KMeansModel):Int ={
    val v :Vector = Vectors.parse(r.getAs(1).toString())
    val prodicti = r.getInt(2)
    prodicti
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
  // 单点轮廓系数 // 轮廓系数 [-1,1] 越大越好
  def  SCSingle(r : Row, clusters:KMeansModel):Double ={
    //预测该点
    val v :Vector = Vectors.parse(r.getAs(1).toString())

    val prodicti = r.getInt(2)
    println(r.toString()+" prodicti is " + prodicti)

    //计算各簇 平均距离
    val clustersDF = clusters.transform(parsedData).select("*").orderBy("label").cache()
    val dtInPoint_k = clustersDF.map(k_Dis_all(v, _, clusters))
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
    println("......................"+v+"的轮廓系数是="+s_i)
    println("----------------------------------------------")

    s_i
  }
  //点到各簇中心内所有点的距离
  def k_Dis_all(p_i: Vector, data: Row, clusters: KMeansModel): (Double, Double, Double) = {
    val this_v = p_i.toArray
    val other_v = Vectors.parse(data.getAs("features").toString).toArray
    //println(dv)
    val pro = data.getInt(2)//"prediction"
    //计算距离 d=sqrt( ∑(xi1-xi2)^ )
    val dis = new EuclideanDistance().compute(this_v, other_v)
    //println("oushi .. "+dis)
    println("点= " + data + " 属于簇：" + pro + " 距离i点=" + dis)
    (pro.toDouble, 1, dis)
  }

  //点到各簇中心内所有点的 平均距离
  def mapavg(d: (Double, (Double, Double))): (Int, Double) = {
    val avg = d._2._2 / d._2._1
    (d._1.toInt, avg)
  }


  // 欧几里得距离 它越小，说明聚类越好
  def ouj(cluster:KMeansModel):Double = {
    val WSSSE:Double = cluster.computeCost(parsedData)
    //println("欧几里得距离= " + WSSSE) //平方误差的和
    WSSSE
  }
  def stop(): Unit ={
    sc.stop()
  }

}
