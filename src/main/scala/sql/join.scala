package sql

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Jary on 2015/10/10.
 */
object join {

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: join <file1> <file2>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("join")//.setMaster("local")
    val sc = new SparkContext(conf)
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    case class Register (d: java.util.Date, uuid: String, cust_id: String, lat: Float,lng: Float)
    case class Click (d: java.util.Date, uuid: String, landing_page: Int)
    val reg = sc.textFile(args(0)).map(_.split("\t")).map(r => (r(1), Register(format.parse(r(0)), r(1), r(2), r(3).toFloat, r(4).toFloat)))
    val clk = sc.textFile(args(2)).map(_.split("\t")).map(c => (c(1), Click(format.parse(c(0)), c(1), c(2).trim.toInt)))
    reg.join(clk).take(2).foreach(print)


    sc.stop()
  }
}
