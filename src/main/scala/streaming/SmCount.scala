package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by spark on 11/28/15.
 */
object TextFileStream {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("smcount").setMaster("local[2]")
    val sc = new StreamingContext(conf,Seconds(1))
    //如果目录中有新创建的文件，则读取
    val line = sc.textFileStream("/home/spark/Desktop/string")
    val word = line.flatMap(_.split(" "))
    val wordcount = word.map(x => (x,1)).reduceByKey(_+_).print()

    sc.start()
    //一直运行，除非人为干预再停止
    sc.awaitTermination()

  }
}
