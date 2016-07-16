package streaming.xiu

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
//http://blog.csdn.net/lovehuangjiaju/article/details/49965857
object QueueStream {

  def main(args: Array[String]) {

    StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("QueueStream").setMaster("local[4]")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    //创建RDD队列
    println("ok 创建RDD队列 ")
    val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]()

    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 3000, 10)
      Thread.sleep(1000)
      //通过程序停止StreamingContext的运行
      //  ssc.stop()
    }
    // Create the QueueInputDStream and use it do some processing
    // 创建QueueInputDStream
    println("ok 创建QueueInputDStream ")

    val inputStream = ssc.queueStream(rddQueue)
    println("ok 处理队列中的RDD数据 ")
    //处理队列中的RDD数据
    val mappedStream = inputStream.map(x => (x % 10, 1))
    println("---mappedStream--- ")
    val r=mappedStream.reduceByKey(_ + _).print()

    //启动计算
    println("ok no ")
    ssc.start()
    println("ok no2 ")
    // Create and push some RDDs into
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 3000, 10)
      Thread.sleep(1000)
      //通过程序停止StreamingContext的运行
    //  ssc.stop()
    }
    println("ok")
    ssc.awaitTermination()
  }
}