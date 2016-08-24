package com.jary.action

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by spark on 11/25/15.
 */
//Date.txt文件定义了日期的分类，将每天分别赋予所属的月份、星期、季度等属性
//日期，年月，年，月，日，周几，第几周，季度，旬、半月
case class tbldate(dateID: String, theyearmonth: String, theyear: String, themonth: String, thedate: String, theweek: String, theweeks: String, thequot: String, thetenday: String, thehalfmonth: String)

//Stock.txt文件定义了订单表头
//订单号，交易位置，交易日期
case class tblstock(ordernumber: String, locationid: String, dateID: String)

//StockDetail.txt文件定义了订单明细
//订单号，行号，货品，数量，金额
case class tblstockdetail(ordernumber: String, rownum: Int, itemid: String, qty: Int, price: String, amount: String)

object SQL_MLbe {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("sql0nmylib").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val stock = sc.textFile("hdfs://master:8000/data/Stock.txt").map(_.split(",")) map (s => tblstock(s(0), s(1), s(2)))
    val tblStock = stock.toDF()
    tblStock.registerTempTable("tblstock")
    sqlContext.cacheTable("tblstock")
    tblStock.printSchema()
    //tblStock.show()

    val date = sc.textFile("hdfs://master:8000/data/Date.txt").map(_.split(",")).map(d => tbldate(d(0), d(1), d(2), d(3), d(4), d(5), d(6), d(7), d(8), d(9))).toDF()
    date.registerTempTable("tbldate")
    sqlContext.cacheTable("tbldate")
    date.printSchema()
    //date.show()

    val stockdetail = sc.textFile("hdfs://master:8000/data/StockDetail.txt").map(_.split(","))
      .map(sd => tblstockdetail(sd(0),
      sd(1).toInt
      , sd(2)
      , sd(3).toInt
      , sd(4)
      , sd(5)))
    val tblStockdetail = stockdetail.toDF()
    tblStockdetail.registerTempTable("tblstockdetail")
    sqlContext.cacheTable("tblstockdetail")

    tblStockdetail.show(100)


    val sqldata = sqlContext.sql("select a.locationid, sum(b.qty) totalqty,sum(b.amount) totalamount from tblstock a join tblstockdetail b on a.ordernumber=b.ordernumber group by a.locationid")//.foreach(println)

    val parseData = sqldata.map{
      case Row(_,to,tom) =>
        val features = Array[Double](to.toString.toDouble,tom.toString.toDouble)
        Vectors.dense(features)
    }

    val numClusters= 3
    val numIterations=20
    val model= KMeans.train(parseData,numClusters,numIterations)

    val resualt2 = sqldata.map{
      case  Row(location,to,tom) =>
        val features = Array[Double](to.toString.toDouble,tom.toString.toDouble)
        val line = Vectors.dense(features)
        val prediction = model.predict(line)
        location+" "+to+" "+tom+" "+prediction
    }.saveAsTextFile("hdfs://master:8000/data/SQL_MLbe.txt")

    sc.stop()

  }
}
