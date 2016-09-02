package utils

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016/8/23 0023.
  */
object WordCount extends App{

//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder.master("local").appName("JavaWordCount").getOrCreate;
//
//    val line = spark.read.textFile(".gitignore").flatmap(_.sptlt(" "))
//          .map(_,1).reduceByKey(_ + _).map(x._2,x._1).sortbykey(fals).map(x._2,x._1).saveAsTextFile("bb")
//
//  }

    Spark_Log()
    val conf = new SparkConf().setAppName("other count").setMaster("local[1]").set("spark.sql.warehouse.dir", "file:///")
    val sc = new SparkContext(conf)
    //val bb = sc.textFile(".gitignore").flatMap(_.sptlt(" ")//.map(_,1).reduceByKey((a:Int,b:Int)=> (a+b)).map(x_2, x._1).sortbykey(false).map(x._2, x._1)

    val data = sc.parallelize(1 to 3)

    val a = data.map(b(_,"hi")).foreach(println(_))

    def b (int: Int,string: String):Array[Int] ={

      val c = data.map(_*2).collect()
      println(c)
      c
    }
    //println(line)
    sc.stop()


}
