package cn.nuecloud.bigdata.dasuan.analysis.k_means

import java.util
import java.util.StringTokenizer

import cn.nuecloud.bigdata.dasuan.utils.dfgenerator.DFGeneratorImpl
import cn.nuecloud.bigdata.utils.{DSLogging, SparkContextImpl, Spark_Log}
import org.apache.commons.math3.ml.distance.EuclideanDistance
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.SparkContext

/**
  * @example {{{
  *
    val sc: SparkContext = SparkContextImpl("SparkContextImpl")
    //mllib/sample_kmeans_data.txt
    val datadir: String = "data/mllib/kmeans_data.txt"
    val  df : DataFrame = ...
    val k = 3 //最大分类数
    val numIterations = 20 //迭代次数
    val a = new K_Means(datadir,sc)
    val clust = a.clusterCenters(k, numIterations)
    val df : DataFram = a.getAnalizedData(clust)
    //打印中心点
    loginfo_a("中心点: "+a.printCenters(clust))
    //计算轮廓系数
    loginfo_a("轮廓系数: "+a.silin(clust).toString)
    // 欧几里得
    loginfo_a("欧几里得: "+a.ouj(clust))
    a.stop()

  }}}
  */

class K_Means(source: Any, @transient sc: SparkContext) extends Serializable with DSLogging {

  // K_Means(datadir: String ,df : DataFrame , @transient sc: SparkContext)
  @transient
  val sqlContext: SQLContext = {
    val sqlContext = new SQLContext(sc)
    sqlContext
  }


  @transient
  var parsedData: DataFrame = null
  source match {
    case d: DataFrame => parsedData = DFGeneratorImpl.dfToVector(d, sc)
    case d: String => parsedData = sqlContext.createDataFrame(paseData(d)).toDF("label", "features")
  }

  //  if (datadir == null && parsedData == null) {
  //    parsedData = DFGeneratorImpl.dfToVector(df, sc)
  //  } else {
  //    parsedData = sqlContext.createDataFrame(paseData()).toDF("label", "features")
  //  }

  private def paseData(datadir: String): Seq[(Int, Vector)] = {

    val people = sc.textFile(datadir).map(tokeNizer).map(p => (0, Vectors.dense(p))).collect()
    val dataseq = people.toSeq
    dataseq
  }
  private def tokeNizer(string: String): Array[Double] = {
    val st = new StringTokenizer(string, " \t\n\n\f,", false);
    val length = st.countTokens()
    val array = new Array[Double](length)
    for (i <- 0 until length if st.hasMoreElements()) {
      array(i) = st.nextElement().toString.toDouble
    }
    array
  }
  def clusterCenters(k: Int, numIterations: Int): KMeansModel = {

    // Cluster the data into two classes using DSKMeans
    val kmeans = new KMeans().setK(k).setFeaturesCol("features")
      .setPredictionCol("prediction")
    //训练模型
    val clusters = kmeans.fit(parsedData)
    // val v = Vectors.dense(1,1,1)
    //clustersDF.filter("features=[0.0,0.0,0.0]").selectExpr("features").show()
    clusters
  }

  def getAnalizedData(km : KMeansModel) : DataFrame={
    //km.transform(parsedData).select("*").show(25)
    val df = km.transform(parsedData).select("features","prediction").toDF()
    df
  }
  // 轮廓系数 // 轮廓系数 [-1,1] 越大越好
  def silin(clusters: KMeansModel): Double = {
    var all = 0.0
    val clustersDF = clusters.transform(parsedData).select("*").orderBy("label").cache()
    val count = clustersDF.count().toInt
    //bigDataLog.analysis(count)
    val sample = clustersDF.take({
      if (Int.MaxValue > count)
        count
      else Int.MaxValue
    })
    for (i <- sample) {
      if (i != null) {
        val singr = SCSingle(i, clusters)
        all += singr
      }
    }
      val sil = f"${(all / count)}%1.3f"
    sil.toDouble
  }

  //预测
  def predict(r: Row, clusters: KMeansModel): Int = {
    val v: Vector = Vectors.parse(r.getAs(1).toString())
    val prodicti = r.getInt(2)
    prodicti
  }

  def printCenters(clusters: KMeansModel): util.ArrayList[String] = {
    val list = new util.ArrayList[String]()
    clusters.clusterCenters.foreach(toList)
    //[[1.34999,0.35000001,0.6], [9.1,9.1,2.09996], [8.587499,6.0875,9.087499]]
    def toList(v: Vector) = {
      val v2s = v.toString
      //loginfo_a("jary1: "+ v2s)
      val splitv = v2s.substring(1,v2s.length-1).split(",")
      val v2ss = new StringBuilder("(")
      for(v <- splitv){
        v2ss.append(f"${v.toDouble}%1.2f" +", ")
      }
      list.add(v2ss.toString().substring(0,v2ss.length-2)+")")
    }
    list
  }

  // 单点轮廓系数 // 轮廓系数 [-1,1] 越大越好
  private def SCSingle(r: Row, clusters: KMeansModel): Double = {
    //预测该点
    val v: Vector = Vectors.parse(r.getAs(1).toString())

    val prodicti = r.getInt(2)
    loginfo_a(r.toString() + " prodicti is " + prodicti)

    //计算各簇 平均距离
    val clustersDF = clusters.transform(parsedData).select("*").orderBy("label").cache()
    val dtInPoint_k = clustersDF.map(k_Dis_all(v, _, clusters))
    val count = dtInPoint_k.map(a => (a._1, a._2)).reduceByKey(_ + _)
    val reduce = dtInPoint_k.map(a => (a._1, a._3)).reduceByKey(_ + _)

    //(0.0,(5.0,5.43840620433566))
    //(1.0,(1.0,12.727922061357855))
    val avg = count.join(reduce).map(mapavg).collect()
    //val vectoarr= avg.toArray
    loginfo_a("all length : " + avg.length)
    for (i <- 0 to avg.length - 1) {
      loginfo_a("all : " + avg {
        i
      })
    }
    // 从各簇中找出 最小和次小 因为最小可能是簇内距离 则次小就是簇外最小
    var min = avg {
      0
    }._2.toDouble //最小值
    var minTh = avg {
      0
    }._2.toDouble //次小

    for (i <- 0 to avg.length - 1) {
      val element = avg {
        i
      }._2
      //zuixiao
      if (element < min) {
        min = element;
      }
    }

    if (min == avg {
      0
    }._2.toDouble) minTh = avg {
      1
    }._2.toDouble
    for (i <- 0 to avg.length - 1) {
      val element = avg {
        i
      }._2
      if (element < minTh && element != min) {
        minTh = element;
      }
    }
    loginfo_a(min + "  <--最小和次小--> " + minTh)
    val avgmap = avg.toMap
    var intavg = avgmap.get(prodicti).getOrElse(0.0)

    //判断最小是不是簇内距离
    var outavg = 0.0
    if (intavg == min) {
      outavg = minTh
    } else {
      outavg = min
    }

    loginfo_a("簇外最小距离：" + outavg + " 簇内距离: " + intavg)
    //取a(i),b*(i)的最大值
    val maxa_b = {
      if (outavg < intavg) {
        intavg
      } else {
        outavg
      }
    }
    loginfo_a("a(i) b*(i)的最大值是：" + maxa_b)
    val s_i = (outavg - intavg) / maxa_b
    loginfo_a("......................" + v + "的轮廓系数是=" + s_i)
    loginfo_a("----------------------------------------------")

    s_i
  }

  //点到各簇中心内所有点的距离
  private def k_Dis_all(p_i: Vector, data: Row, clusters: KMeansModel): (Double, Double, Double) = {
    val this_v = p_i.toArray
    val other_v = Vectors.parse(data.getAs("features").toString).toArray
    //bigDataLog.analysis(dv)
    val pro = data.getInt(2) //"prediction"
    //计算距离 d=sqrt( ∑(xi1-xi2)^ )
    val dis = new EuclideanDistance().compute(this_v, other_v)
    //bigDataLog.analysis("oushi .. "+dis)
    loginfo_a("点= " + data + " 属于簇：" + pro + " 距离i点=" + dis)
    (pro.toDouble, 1, dis)
  }

  //点到各簇中心内所有点的 平均距离
  private def mapavg(d: (Double, (Double, Double))): (Int, Double) = {
    val avg = d._2._2 / d._2._1
    (d._1.toInt, avg)
  }


  // 欧几里得距离 它越小，说明聚类越好
  def ouj(cluster: KMeansModel): Double = {
    val WSSSE: Double = cluster.computeCost(parsedData)
    //bigDataLog.analysis("欧几里得距离= " + WSSSE) //平方误差的和
    WSSSE
  }

  def stop(): Unit = {
    this.sc.stop()
  }

}


private[dasuan] object TKMeans extends DSLogging{

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = SparkContextImpl("SparkContextImpl")
    //mllib/sample_kmeans_data.txt
    val datadir: String = "data/mllib/kmeans_data.txt"
    val sqlContext = new SQLContext(sc)
    // Crates a DataFrame
    val dataset: DataFrame = sqlContext.createDataFrame(Seq(
      (1, Vectors.dense(0.0, 0.0, 0.0)),
      (2, Vectors.dense(0.1, 0.1, 0.1)),
      (3, Vectors.dense(0.2, 0.2, 0.2)),
      (4, Vectors.dense(9.0, 9.0, 9.0)),
      (5, Vectors.dense(9.1, 9.1, 9.1)),
      (6, Vectors.dense(9.2, 9.2, 9.2))
    )).toDF("id", "features")

    //val  df : DataFrame = ...
    val k = 3 //最大分类数
    val numIterations = 20 //迭代次数
    val a = new K_Means(datadir,sc)

    datadir.foreach(println)

    val clust = a.clusterCenters(k, numIterations)
    //打印中心点
    loginfo_a("中心点: "+a.printCenters(clust))
    //计算轮廓系数
    loginfo_a("轮廓系数: "+a.silin(clust).toString)
    // 欧几里得
    loginfo_a("欧几里得: "+a.ouj(clust))
    logError("")
    sc.stop()
  }
}
