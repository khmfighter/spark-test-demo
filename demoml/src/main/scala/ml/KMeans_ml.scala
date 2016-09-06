/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ml

// scalastyle:off println

// $example on$
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import utils.Spark_Log
// $example off$
import org.apache.spark.sql.SparkSession

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
  val spark = {
    val spark = SparkSession
      .builder.master("local[4]")
      .appName(s"${this.getClass.getSimpleName}").config("spark.sql.warehouse.dir", "file:///")
      .getOrCreate()
    spark
  }
  @transient
  var parsedData  ={
    val parsedData = spark.read.format("libsvm").load(datadir)
    parsedData
  }

  def clusterCenters(k:Int,numIterations:Int) : KMeansModel= {
    // Load and parse the data
    // Cluster the data into two classes using KMeans
    val kmeans = new KMeans().setK(k).setMaxIter(numIterations).setSeed(1L).setFeaturesCol("features").setPredictionCol("prediction")
    //训练模型
    val clusters = kmeans.fit(parsedData)
    //计算中心点
    clusters.clusterCenters.foreach(println)
    println(clusters.getPredictionCol)

    clusters
  }
  // 欧几里得距离 它越小，说明聚类越好
  def ouj(cluster:KMeansModel):Double = {
    val WSSSE:Double = cluster.computeCost(parsedData)
    //println("欧几里得距离= " + WSSSE) //平方误差的和
    WSSSE
  }


  def stop(): Unit ={
    spark.stop()
  }

}
