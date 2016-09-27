package dataset

import org.apache.spark.sql.SparkSession
import utils.Spark_Log

/**
  * Created by Administrator on 2016/9/5 0005.
  */
class SparkSQLExample {


}

object SparkSQLExample{
  case class Ps(name : String ,age :Int)
  Spark_Log()
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[1]").appName(this.getClass.getName).config("spark.sql.warehouse.dir", "file:///").getOrCreate()

    val peopleDF = spark.read.format("json").load("data/people.json")
    peopleDF.select("name", "age").write.format("parquet").save("darta/namesAndAges.parquet")

//    val df = spark.read.json("data/people.json")
//    df.show()
//    df.printSchema()
//    df.select("name").show()
//
//    df.createOrReplaceTempView("p")
//    val sqlDF = spark.sql("select * from p")
//    sqlDF.show()

    import spark.implicits._


    spark.stop()

  }
}