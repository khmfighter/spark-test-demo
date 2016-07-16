package streaming.xiu

import java.io.File


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}


/**
 * Created by spark on 12/24/15.
 */
object CheckPoint {
  def createContext(ip: String, port: String, outputPath: String, checkpointDirectory: String): StreamingContext = {
    //程序第一运行时会创建该条语句，如果应用程序失败，则会从checkpoint中恢复，该条语句不会执行
    println("Creating new context")
    val outputFile = new File(outputPath)
    if (outputFile.exists()) outputFile.delete()
    val sparkConf = new SparkConf().setAppName("RecoverableNetworkWordCount").setMaster("local[4]")
    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(checkpointDirectory)

    //将socket作为数据源
    val lines = ssc.socketTextStream(ip, port.toInt)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
      val counts = "Counts at time " + time + " " + rdd.collect().mkString("[", ", ", "]")
      println(counts)
      println("Appending to " + outputFile.getAbsolutePath)
     // Files.append(counts + "\n", outputFile, Charset.defaultCharset())
    })
    ssc
  }

  //将String转换成IntprivateobjectIntParam {
  def unapply(str: String): Option[Int] = {
    try {
      Some(str.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println("You arguments were " + args.mkString("[", ", ", "]"))
      System.err.println(
        """
          |Usage: RecoverableNetworkWordCount <hostname> <port> <checkpoint-directory>
          |     <output-file>. <hostname> and <port> describe the TCP server that Spark
          |     Streaming would connect to receive data. <checkpoint-directory> directory to
          |     HDFS-compatible file system which checkpoint data <output-file> file to which the
          |     word counts will be appended
          |
          |In local mode, <master> should be 'local[n]' with n > 1
          |Both <checkpoint-directory> and <output-file> must be absolute paths
        """.stripMargin
      )
      System.exit(1)
    }
    val Array(ip, port, checkpointDirectory, outputPath) = args

    //getOrCreate方法，从checkpoint中重新创建StreamingContext对象或新创建一个StreamingContext对象
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,() => {
      createContext(ip, port, outputPath, checkpointDirectory)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
