package com.jary.error

import breeze.linalg.sum
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by spark on 11/15/15.
 */
object Median {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Median").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("")
    val mapeddata = data.map(num => {
      ( sum*(1/1000),num)
    })

    val count = mapeddata.reduceByKey((a,b) => {
      a+b
    }).collect()

    val sum_count = count.map(data => {data._2}).sum

    var temp =0
    var index =0

    var mid = sum_count/2
    for (i <- 0 to 10){
      temp = temp + count(i)
      if(temp <= mid ){
        index = i
        break
      }
    }

    val offerset = temp-mid
    val result = mapeddata.filter(num => num._1 == num).takeOrdered(offerset)
    println("Median is "+ result(offerset))
    sc.stop()

  }
}
