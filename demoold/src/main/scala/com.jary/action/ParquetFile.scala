package com.jary.action

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by spark on 11/21/15.
 */
case class log(ip: String, ps: String)
object ParquetFile {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("asdfas").setMaster("local")
    println("here....")
    val sc = new SparkContext(conf)

    println("here....3")
    val sqlContext = new  org.apache.spark.sql.SQLContext(sc)

    //hdfs://master:8000/data/people.txt
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val data = sc.textFile("hdfs://master:8000/data/log.txt").map(_.split("\t")).filter(p => p(14).length > 4 && p(4).length > 1).map(p => log(p(4), p(14).substring(1, p(14).lastIndexOf("]"))))

    val dfdata = data.toDF()

    //pqrquet file  store to current path or dfs
    //dfdata.saveAsParquetFile("hdfs://master:8000/data/log.parquet")
    val parqueFile = sqlContext.parquetFile("hdfs://master:8000/data/log.parquet")

    parqueFile.registerTempTable("log")

    val teenagers = sqlContext.sql("SELECT * FROM log ")

//    val traing = teenagers.map{
//
//      row => val feturse = Array[String]("fwrdws","32qr","3r23r")
//
//
//        //function
//    }



    val count = teenagers.count().toInt

    println("count:"+count)

    sc.stop()
  }
}
