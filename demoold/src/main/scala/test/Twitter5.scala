package test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by spark on 11/30/15.
 */

/**
 * Created by Jary on 2015/11/13.
 */
object Twitter5 {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Twitter5").setMaster("local")

    val sc = new SparkContext(conf)

    //hdfs://master:8000/data/twitter.txt

    val user  = sc.textFile("hdfs://master:8000/data/twitter5.txt").map(_.split(" ")).map(m => (m(0),1)).reduceByKey(_+_).map(x => (x._2,x._1)).sortByKey().map(x => (x._2,x._1)).foreach(println)//.saveAsTextFile("hdfs://master:8000/output/twitter")


    sc.stop()


  }
}

