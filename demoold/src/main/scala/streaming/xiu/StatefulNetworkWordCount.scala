package streaming.xiu

/**
 * Created by spark on 12/22/15.
 * 如果想要连续地进行计数，则可以使用updateStateByKey方法来进行。
 * 下面的代码主要给大家演示如何updateStateByKey的方法，
 *
 * http://blog.csdn.net/lovehuangjiaju/article/details/50042621
 *
 * nc -lk 9999
 */
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming._

object StatefulNetworkWordCount {
  def main(args: Array[String]) {

    //函数字面量，输入的当前值与前一次的状态结果进行累加
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum

      val previousCount = state.getOrElse(0)

      Some(currentCount + previousCount)
    }

    //输入类型为K,V,S,返回值类型为K,S
    //V对应为带求和的值，S为前一次的状态
    val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
    }

    val sparkConf = new SparkConf().setAppName("StatefulNetworkWordCount").setMaster("local[4]")

    //每一秒处理一次
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    //当前目录为checkpoint结果目录，后面会讲checkpoint在Spark Streaming中的应用
    ssc.checkpoint("out/re")

    //RDD的初始化结果
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))


    //使用Socket作为输入源，本例ip为localhost，端口为9999
    val lines = ssc.socketTextStream("master", 9999)
    //flatMap操作
    val words = lines.flatMap(_.split(" "))
    //map操作
    val wordDstream = words.map(x => (x, 1))

    //updateStateByKey函数使用
    val stateDstream = wordDstream.updateStateByKey[Int](newUpdateFunc,
      new HashPartitioner (ssc.sparkContext.defaultParallelism), true, initialRDD)
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
