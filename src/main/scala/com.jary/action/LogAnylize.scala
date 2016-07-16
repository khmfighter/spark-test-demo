package com.jary.action

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by spark on 11/18/15.
 */
case class log(ip: String, ps: String)

object LogAnylize {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("LogAnylize").setMaster("local")
    println("here....")
    val sc = new SparkContext(conf)

    println("here....3")
    val sqlContext = new SQLContext(sc)

    //hdfs://master:8000/data/people.txt
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val data = sc.textFile("hdfs://master:8000/data/log.txt").map(_.split("\t")).filter(p => p(14).length > 4 && p(4).length > 1).map(p => log(p(4), p(14).substring(1, p(14).lastIndexOf("]"))))

    val dfdata = data.toDF()



    //    val c4 = data[4]
    //    val c4 =data.filter(data.map(l => l.length>4))
    //
    dfdata.registerTempTable("log")
    //val f = data.foreach(println(_))


    println("selecet...")
    val teenagers = sqlContext.sql("SELECT * FROM log ").cache()
    //teenagers.collect().foreach(println)
    val count = teenagers.count().toInt

    println("count:"+count)
    val pkgArray = teenagers.map(t => t(1)).take(count)
    val ipArray = teenagers.map(t => t(0)).take(count)
   // val logMap = new HashMap[String, Array]

    for (x <- pkgArray) {
      println
      println
      println
      val str= x.toString

      val str2 = str.replace(",{","\n{")
      println(str2)
      val varay = str2.split("\n")
      for(tt <- varay) {
        if(!"".equals(tt)){
          var sst ="e"
          try{

             sst = tt.substring(tt.indexOf("packname\":\"") + 11, tt.indexOf("\",\"version"))
          }catch{

            case _  => sst = tt.substring(tt.indexOf("packname\":\"") + 11, tt.length-2)
          }

          println(sst)
      }

      }
     // var str2 = str.replace(",{","\n{")
     // str.substring(str.indexOf("packname\":\"")+11,str.indexOf("\",\"version"))

      //println(str)


    }




    //
    //    // teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")
    //
    //    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    //    // The columns of a row in the result can be accessed by field index:
    //    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
    //
    //    // or by field name:
    //    teenagers.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    //teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
    // Map("name" -> "Justin", "age" -> 19)

    sc.stop()

  }
}
