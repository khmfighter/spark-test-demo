package sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by spark on 11/25/15.
 */

//apply schmea way
object SQLOnSpark2 {

  def main(args: Array[String]) {
    println("start....")
    val conf = new SparkConf().setAppName("sqlonspark2").setMaster("local")
    println("conf ... next sc")
    val sc = new SparkContext(conf)
    println("sqlcontext")
    val sqlContext = new SQLContext(sc)

    //creat schema
    val schemaString = "name age"
    val schem = StructType(schemaString.split(" ").map(fn => StructField(fn,StringType,true)))

    //create rowRDD
    val rowRDD = sc.textFile("hdfs://master:8000/data/people.txt").map(_.split(",")).map(p => Row(p(0),p(1).trim))

    val rddPeople = sqlContext.applySchema(rowRDD,schem)
    rddPeople.registerTempTable("people")


    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 10 and age <= 19")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
  }
}
