package com.jary.error

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by spark on 11/15/15.
 */
object TopK {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("topk").setMaster("local")
    val sc = new SparkContext(conf)

    val count = sc.textFile("data").flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    val topk = count.mapPartitions(iter => {

      while (iter.hasNext) {
        putToHeap(iter.next())
      }
      getHeap().iterator
    }).collect()

    val iter = topk.iterator
    while (iter.hasNext) {
      putToHeap(iter.next())
    }

    val outiter = getHeap.iterator


    println("Topk value")
    while (outiter.hasNext) {
      println("\n words :" + outiter.next()._1 + " num: " + outiter.next()._2)
    }

    sc.stop()

  }

  def putToHeap(iter: (String, Int)): Unit = {

  }

  def getHeap(): Array[(String, Int)] = {
    val a = new Array[(String, Int)]()

  }


}
