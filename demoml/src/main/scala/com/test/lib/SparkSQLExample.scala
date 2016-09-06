package com.test.lib

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, Word2Vec}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
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

    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.select("result").take(3).foreach(println)


    spark.stop()

  }
}