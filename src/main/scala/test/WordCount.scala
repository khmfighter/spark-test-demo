package test

/**
 * Created by spark on 11/19/15.
 */
object WordCount {
  // sc.textFile("hdfs://").flatmap(_.sptlt(" ")).map(_,1).reduceByKey(_ + _).saveAsTextFile("hdfs://")
  //k -v hu huan sort
  //map(x._2,x._1).sortbykey(fals).map(x._2,x._1)

  def main(args: Array[String]) {

    // sc.textFile("hdfs://").flatmap(_.sptlt(" ")).map(_,1).reduceByKey(_ + _).map(x._2,x._1).sortbykey(fals).map(x._2,x._1).saveAsTextFile("hdfs://")
  }

}
