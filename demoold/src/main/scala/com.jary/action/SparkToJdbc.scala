package com.jary.action

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD

/**
 * Created by Jary on 2015/11/19.
 */
object SparkToJDBC {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkToJDBC").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = new JdbcRDD(

      sc,() => {

        Class.forName("com.mysql.jdbc.Driver").newInstance()
        DriverManager.getConnection("jdbc:mysql://192.168.137.101:3306/overseaads", "root", "000000")
      },
      "SELECT pkg FROM o_apk WHERE id >= ? AND id <= ?", 1, 100, 1,
      r => r.getString(1)).cache()

    rdd.foreach(println)

    sc.stop()

  }


}
