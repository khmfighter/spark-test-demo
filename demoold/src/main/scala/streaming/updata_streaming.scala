package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by spark on 11/30/15.
 */


//reciver data from object soket
object UpdateStateByKey {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val updataFunc = (values: Seq[Int], stat: Option[Int]) => {
      val currentcount = values.foldLeft(0)(_ + _)
      val previousCount = stat.getOrElse(0)
      Some(currentcount + previousCount)

    }
    val conf = new SparkConf().setAppName("updata_streaming").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("out/result")

    val lines = ssc.socketTextStream("master", 9999)
    val word = lines.flatMap(_.split(","))
    val wordC = word.map(x => (x, 1))

    val stateDsteam = wordC.updateStateByKey[Int](updataFunc)
    stateDsteam.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
