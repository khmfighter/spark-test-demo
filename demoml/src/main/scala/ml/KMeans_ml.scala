package ml

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.Spark_Log
// $example off$

/**
 * An example demonstrating k_means clustering.
 * Run with
 * {{{
 * bin/run-example ml.KMeansExample
 * }}}
 */
object KMeans_ml {
  //System.setProperty("hadoop.home.dir", "C:\\Program Files (x86)\\hadoop-common-2.2.0-bin-master\\bin");
  Spark_Log()
  def main(args: Array[String]): Unit = {
    //mllib/sample_kmeans_data.txt
    val datadir: String = "data/mllib/sample_kmeans_data.txt"
    val k = 2  //最大分类数
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
  var parsedData  ={
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
    // Load and parse the data
    // Cluster the data into two classes using KMeans
    val kmeans = new KMeans().setK(k).setFeaturesCol("features")
      .setPredictionCol("prediction")
    //训练模型
    val clusters = kmeans.fit(parsedData)
    //计算中心点
    clusters.clusterCenters.foreach(println)
//    val v = Vectors.dense(1,1,1)

    val df = clusters.transform(parsedData)
    df.orderBy()
    val part = df.selectExpr("*").orderBy("prediction").show()
    df.filter("features=[0.0,0.0,0.0]").selectExpr("features").show()

    clusters
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
