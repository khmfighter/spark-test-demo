package sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Jary on 2015/11/13.
 */
case class Person(name: String, age: Int)
object SQLOnSpark {

  def main(args: Array[String]) {

    println("here....")
    val conf = new SparkConf().setAppName("SQLOnSpar").setMaster("local[2]")
    println("here....")
    val sc = new SparkContext(conf)

    println("here....")
    val sqlContext = new SQLContext(sc)

    //hdfs://master:8000/data/people.txt
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val people  = sc.textFile("/home/spark/Desktop/data/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()

    people.registerTempTable("people")

    sqlContext.cacheTable("people")


  //  people.printSchema()
   // people.show()
  //  people.select("name").show()

    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 10 and age <= 19")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

    // teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")

    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index:
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

    // or by field name:
    teenagers.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    //teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
    // Map("name" -> "Justin", "age" -> 19)

    sc.stop()


  }
}


