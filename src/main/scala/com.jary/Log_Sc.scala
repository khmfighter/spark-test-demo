package com.jary

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by spark on 12/2/15.
 */
object Log_Sc {

  def apply(){
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  }

  def lo_Sc(s:String): SparkContext ={
    val conf = new SparkConf().setAppName(s).setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc
  }
}
