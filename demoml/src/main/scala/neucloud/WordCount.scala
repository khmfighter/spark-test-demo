package neucloud

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import utils.Spark_Log

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
  def main(args: Array[String]): Unit = {

    for (1 <- 1 to 100) {
      println("fdasdf")
    }//
  }
  def main3(args: Array[String]): Unit = {


    Spark_Log()
    val conf = new SparkConf().setAppName("other count").setMaster("spark://hadoop1:7077")
    //.set("spark.sql.warehouse.dir", "file:///")
    val sc = new SparkContext(conf)
    //val bb = sc.textFile(".gitignore").flatMap(_.sptlt(" ")//.map(_,1).reduceByKey((a:Int,b:Int)=> (a+b)).map(x_2, x._1).sortbykey(false).map(x._2, x._1)

    //println(line)
    sc.stop()

  }
}
