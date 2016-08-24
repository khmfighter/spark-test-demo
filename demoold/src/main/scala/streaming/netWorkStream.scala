package streaming

import com.jary.Log_Sc
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by spark on 11/30/15.
 */
//reciver data from object soket
object NetSocketTextStream {

  def main(args: Array[String]) {
    //Log_Sc()
    val conf = new SparkConf().setAppName("networkstream").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(3))
    //创建SocketInputDStream，接收来自ip:port发送来的流数据
    val lines = ssc.socketTextStream("master",9999,StorageLevel.MEMORY_AND_DISK)
    val words = lines.flatMap(_.split(" "))

    val wordc= words.map(x => (x,1)).reduceByKey(_+_)

    wordc.print()
    println("socket:")
    ssc.start()
    ssc.awaitTermination()
/**
 * socket:
15/12/22 18:08:05 WARN ReceiverSupervisorImpl: Restarting receiver with delay 2000 ms: Socket data stream had no more data
15/12/22 18:08:05 ERROR ReceiverTracker: Deregistered receiver for stream 0: Restarting receiver with delay 2000ms: Socket data stream had no more data
15/12/22 18:08:07 WARN ReceiverSupervisorImpl: Receiver has been stopped
15/12/22 18:08:07 WARN ReceiverSupervisorImpl: Receiver has been stopped
15/12/22 18:08:07 WARN BlockGenerator: Cannot stop BlockGenerator as its not in the Active state [state = StoppedAddingData]
-------------------------------------------
Time: 1450836471000 ms
-------------------------------------------

-------------------------------------------
Time: 1450836474000 ms
-------------------------------------------
(Michael,,2)
(29,2)

-------------------------------------------
Time: 1450836477000 ms
-------------------------------------------
(Andy,,3)
(30,3)

-------------------------------------------
Time: 1450836480000 ms
-------------------------------------------
(Andy,3)
(30,3)

-------------------------------------------
Time: 1450836483000 ms
-------------------------------------------
(19,2)
(Andy,,1)
(Justin,,2)
(30,1)

-------------------------------------------
Time: 1450836486000 ms
-------------------------------------------
(19,1)
(Andy,,1)
(Justin,,1)
(30,1)


 */
  }
}
