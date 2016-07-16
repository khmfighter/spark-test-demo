package sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by spark on 11/25/15.
 */
object HiveObSpark {

  def main(args: Array[String]) {


  val conf = new SparkConf().setAppName("sqlonspark2").setMaster("local")
  println("conf ... next sc")
  val sc = new SparkContext(conf)
  println("sqlcontext")

  val hiveContext = new HiveContext(sc)
  hiveContext.sql("use saledata")
  hiveContext.sql("show tables").collect().foreach(println)

  }
}
