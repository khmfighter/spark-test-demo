package test

import _root_.org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by spark on 12/4/15. 4525224k total,
 *                              2192540k

 */
object RDD_Test {

  def main(args: Array[String]) {

 // Log_Sc()
    val conf = new SparkConf().setAppName("rdd_text")//.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(1 to 5)
    val rdd2 = sc.parallelize( 4.to(8))

    val d = rdd1.union(rdd2).map((_,1)).groupByKey().collect()
    for (dd <- d){
      val a = dd._2.toList
      for(a1 <-a) println(a1)
    }

  }
}
