package sql

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Jary on 2015/10/10.
 */
object SogouA {

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: SogouA <file1> <file2>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("SogouA")
    val sc = new SparkContext(conf)


    //val rdd1 = sc.textFile("hdfs://hadoop1:8000/dataguru/data/SogouQ1.txt")
    //    val rdd1 = sc.textFile(args(0))
    sc.textFile(args(0)).map(_.split("\t")).filter(_.length == 6).map(x => (x(1), 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1)) saveAsTextFile (args(1))

    sc.stop()
  }
}















