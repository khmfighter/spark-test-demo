package com.jary.streaming


import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by spark on 1/28/16.
 */

case class Adid(id: String)

object flumescala {

  def main(args: Array[String]) {

    //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    // Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val updataFunc = (values: Seq[Int], stat: Option[Int]) => {
      val currentcount = values.foldLeft(0)(_ + _)
      val previousCount = stat.getOrElse(0)
      Some(currentcount + previousCount)

    }

    val conf = new SparkConf().setAppName("flumescala").setMaster("local")//("spark://master99:7077")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    //ssc.checkpoint("/data/tools/bigdata/spark/checkpoint/flumescala2/")
    //ssc.checkpoint("hdfs://52.76.28.252:8000/tmp/flume")
    //54.169.187.99 172.31.7.166 192.168.137.100
    val stream = FlumeUtils.createStream(ssc, "192.168.137.100", 60044, StorageLevel.MEMORY_ONLY)

    if (stream.count() != 0) {

      val event = stream.flatMap(cnt => {
        val data = new String(cnt.event.getBody().array())
        Some(data)
      })

      val split = event.filter(x => x.contains("postback") && x.contains("-"))
      split.print()
      //  val sum =split.map(_.split("-")).filter(_.length==3).map(x => (x(2), 1)).reduceByKey(_ + _)
      val sum = split.map(_.split("-")).filter(_.length==3).map(x => (x(2), 1)).updateStateByKey[Int](updataFunc)
      sum.print()

      stream.count().map(cnt => "Received " + cnt + " ").print()

    }

    ssc.start()
    ssc.awaitTermination()

  }
}
