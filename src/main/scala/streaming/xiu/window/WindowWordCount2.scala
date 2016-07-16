package streaming.xiu.window

/**
 * Created by spark on 12/22/15.
 */
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import streaming.xiu.StreamingExamples

object CountByWindow {
  def main(args: Array[String]) {

    StreamingExamples.setStreamingLogLevels()

    val conf = new SparkConf().setAppName("WindowWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 创建StreamingContext
    val ssc = new StreamingContext(sc, Seconds(2))
    // 定义checkpoint目录为当前目录
    ssc.checkpoint(".")


    val lines = ssc.socketTextStream("master", 9999, StorageLevel.MEMORY_ONLY_SER)
    val words = lines.flatMap(_.split(" "))

    //countByWindow  countByWindow方法计算基于滑动窗口的DStream中的元素的数量。
    val countByWindow=words.countByWindow(Seconds(30), Seconds(2))

    countByWindow.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
