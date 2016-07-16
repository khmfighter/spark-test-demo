package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by spark on 11/30/15.
 */
object WindowsCount {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf  = new SparkConf().setAppName("windowsCounr").setMaster("local[2]")
    val  sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(1))

    ssc.checkpoint("out/checkpoint")

    val lines  = ssc.socketTextStream("master",9999,StorageLevel.MEMORY_AND_DISK)

    val words  = lines.flatMap(_.split(","))

    val wordCount = words.map(x => (x,1)).reduceByKeyAndWindow((a:Int,b:Int)=> (a+b),Seconds(4),Seconds(1))

    wordCount .print()
    ssc.start()
    ssc.awaitTermination()



  }
}
