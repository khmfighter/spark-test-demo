package streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
 * Created by spark on 1/25/16.
 */

object FlumeEventPrint {
  def main(args: Array[String]) {

    //val Array(host, port) = args
    val batchInterval = Milliseconds(2000)

    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("FlumeEventCount").setMaster("local")
    val ssc = new StreamingContext(sparkConf, batchInterval)

    // Create a flume stream
    val stream = FlumeUtils.createStream(ssc, "192.168.137.100", 33333, StorageLevel.MEMORY_AND_DISK)
    //stream.saveAsTextFiles("nothing")
    // Print out the count of events received from this server in each batch
    //stream.map{cnt => "Received " + cnt mkString " " + " flume events." }.saveAsTextFiles("nothing/")
    stream.map(e=>e.event)saveAsTextFiles("nothing/")

    ssc.start()
    ssc.awaitTermination(10000)
    ssc.stop(true,true)
  }


}
