package streaming.xiu

import com.jary.Log_Sc
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by spark on 12/22/15.
 */
object BasicSources {
  def main(args: Array[String]) {
    Log_Sc()

    val conf = new SparkConf().setAppName("BasicSources").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc,Seconds(2))

    ssc.fileStream("/home/spark/Desktop/string")

    //ssc.actorStream(props = ,"sd"stor)

    ssc.start()
    ssc.awaitTermination()
  }

}
