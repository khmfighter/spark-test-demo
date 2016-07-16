package test

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by spark on 11/19/15.
 */
object WordCount {
  // sc.textFile("hdfs://").flatmap(_.sptlt(" ")).map(_,1).reduceByKey(_ + _).saveAsTextFile("hdfs://")
  //k -v hu huan sort
  //map(x._2,x._1).sortbykey(fals).map(x._2,x._1)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("other count").setMaster("local[2]")
    val sc = new SparkContext(conf)

//    sc.textFile(".gitignore").flatmap(_.sptlt(" "))
//      .map(_,1).reduceByKey(_ + _).map(x._2,x._1).sortbykey(fals).map(x._2,x._1).saveAsTextFile("bb")
  }

}
