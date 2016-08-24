package com.test.lib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2016/8/23 0023.
  */
object WordCount {

//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder.master("local").appName("JavaWordCount").getOrCreate;
//
//    val line = spark.read.textFile(".gitignore").flatmap(_.sptlt(" "))
//          .map(_,1).reduceByKey(_ + _).map(x._2,x._1).sortbykey(fals).map(x._2,x._1).saveAsTextFile("bb")
//
//  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("other count").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //val bb = sc.textFile(".gitignore").flatMap(_.sptlt(" ")//.map(_,1).reduceByKey((a:Int,b:Int)=> (a+b)).map(x_2, x._1).sortbykey(false).map(x._2, x._1)
    val line = sc.textFile("data/ml").flatMap(_.split(" ")).map(x => (x,1)).foreach(println(_))

    //println(line)

  }
}
