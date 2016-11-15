package traing.weekend

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016/8/23 0023.
  */
object WordCount extends App {

  println("asa")
  a("asfa",22)

  def a(sss: => Unit,b : Int) = {
    println(b)
    println(sss)
  }

  //  def main(args: Array[String]): Unit = {
  //    val spark = SparkSession.builder.master("local").appName("JavaWordCount").getOrCreate;
  //
  //    val line = spark.read.textFile(".gitignore").flatmap(_.sptlt(" "))
  //          .map(_,1).reduceByKey(_ + _).map(x._2,x._1).sortbykey(fals).map(x._2,x._1).saveAsTextFile("bb")
  //
  //  }
  def main1(args: Array[String]): Unit = {
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("other count").setMaster("local[4]") //.setMaster("spark://hadoop1:7077")
    //.set("spark.sql.warehouse.dir", "file:///")
    val sc = new SparkContext(conf)
    val t = sc.parallelize(1 to 10).foreach(a => (println("this is a " + a + " ...................... ")))
    val a = sc.getConf.get("spark.mas")
    println(a)
    sc.stop()

  }

  def w(): Int = {
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("other count") //.setMaster("spark://hadoop1:7077")
    //.set("spark.sql.warehouse.dir", "file:///")
    val sc = new SparkContext(conf)
    val t = sc.parallelize(1 to 100000).foreach(a => (println("this is a " + a + " ...................... ")))

    sc.stop()
    88
  }
}
