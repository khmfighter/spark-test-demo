package streaming.xiu.window

/**
 * Created by spark on 12/22/15.
 *
 * WindowWordCount——reduceByKeyAndWindow方法使用
 */
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import streaming.xiu.StreamingExamples

object ReduceByKeyAndWindow {
  def main(args: Array[String]) {
    //传入的参数为localhost 9999 30 10

    StreamingExamples.setStreamingLogLevels()

    val conf = new SparkConf().setAppName("WindowWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 创建StreamingContext，batch interval为5秒
    val ssc = new StreamingContext(sc, Seconds(5))


    //Socket为数据源
    val lines = ssc.socketTextStream("master", 9999, StorageLevel.MEMORY_ONLY_SER)

    val words = lines.flatMap(_.split(" "))

    // windows操作，对窗口中的单词进行计数
    val wordCounts = words.map(x => (x , 1)).reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30), Seconds(10))

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}