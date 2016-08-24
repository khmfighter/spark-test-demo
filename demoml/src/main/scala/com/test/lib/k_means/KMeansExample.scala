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

package com.test.lib.k_means

// scalastyle:off println

// $example on$
import org.apache.spark.ml.clustering.KMeans
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example demonstrating k_means clustering.
 * Run with
 * {{{
 * bin/run-example ml.KMeansExample
 * }}}
 */
object KMeansExample {
  //System.setProperty("hadoop.home.dir", "C:\\Program Files (x86)\\hadoop-common-2.2.0-bin-master\\bin");
  //Spark_Log.apply()
  def main(args: Array[String]): Unit = {
   // Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
   // Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

//    val conf = new SparkConf().setAppName("sqlonspark2").setMaster("local")
//    val sc = new SparkContext(conf)


    val spark = SparkSession
      .builder.master("local[4]")
      .appName(s"${this.getClass.getSimpleName}").config("spark.sql.warehouse.dir","file:///")
      .getOrCreate()

    // $example on$
    // Loads data.
    val dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")

    // Trains a k_means model.
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(dataset)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
