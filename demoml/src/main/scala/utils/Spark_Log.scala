package utils

import org.apache.log4j.{Level, Logger}

/**
  * Created by Administrator on 2016/8/24 0024.
  */
object Spark_Log {

  def apply (): Unit ={
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  }
//
//    def lo_Sc(s:String): SparkContext ={
//      val conf = new SparkConf().setAppName(s).setMaster("local[2]")
//      val sc = new SparkContext(conf)
//      sc
//    }
}
