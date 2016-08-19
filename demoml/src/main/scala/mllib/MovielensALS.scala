package mllib

import java.io.File

import com.jary.Log_Sc
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source
;

/**
 * Created by spark on 12/2/15.
 */
object MovielensALS {

  def main(args: Array[String]) {
    Log_Sc()

    val conf = new SparkConf().setAppName("MovielensALS").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val myRatings = loadRatings("/home/spark/Desktop/data/demo.app.mllib/personalRatings.txt")
    val myRatingsRDD = sc.parallelize(myRatings, 1)

    val movieLensHomedir ="/home/spark/Desktop/data/demo.app.mllib/medium/"

    val ratings = sc.textFile(new File(movieLensHomedir, "ratings.dat").toString).map({
      lines => val fields = lines.split("::")
        (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    })

    val movies = sc.textFile(new File(movieLensHomedir, "movies.dat").toString).map {
      line =>
        val fileds = line.split("::")
        (fileds(0).toInt, fileds(1))

    }.collect().toMap

    val numRatings = ratings.count()
    val numUsers = ratings.map(_._2.user).distinct().count()
    val numMovies = ratings.map(_._2.product).distinct().count()

    println("got " + numRatings + " rating from " + numUsers + " users on " + numMovies + " movies.")

    val numPartitins = 1
    val training = ratings.filter(x => x._1 < 6)
      .values
      .union(myRatingsRDD)
      .repartition(numPartitins)
      .cache()
    val validation = ratings
      .filter(x => x._1 >= 6 && x._1 < 8)
      .values
      .repartition(numPartitins)
      .cache()
    val test = ratings.filter(x => x._1 >= 8).
      values
      .cache()

    val numTraining = training.count()
    val numValidation = validation.count()
    val numtest = test.count()
    println("Traing:" + numTraining + " , validation:" + numValidation + " , test:" + numtest)


    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)

    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambdas = -1.0
    var bestNumIter = -1

    for (rank <- ranks; lambda <- lambdas; numiter <- numIters) {
      val model = ALS.train(training, rank, numiter, lambda)
      val validationRmse = computeRmse(model, validation, numValidation)
      println("RMSE (validation) = " + validationRmse + "for the model trained with rank =" + rank + ",lanbda= " + lambda + ", and Numiter = " + numiter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambdas = lambda
        bestNumIter = numiter
      }
    }

    val testRmse = computeRmse(bestModel.get, test, numtest)
    println("the best model was teained with rank = " + bestRank + " and lamda = " + bestLambdas + ", and numiter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")

    val meanRating = training.union(validation).map(_.rating).mean()
    val baselineRmse = math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean())
    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    println(" the best model improves the baselkine by " + "%1.2f".format(improvement) + "%.")

    val myRatedMovieIds = myRatings.map(_.product).toSet
    val candidates = sc.parallelize(movies.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
    val recommendations = bestModel.get.predict(candidates.map((0, _)))
      .collect().sortBy(-_.rating).take(10)
    var i = 1
    println("moveies recommemdation for u is :")
    recommendations.foreach { r =>
      println("%2d".format(i) + ":" + movies(r.product))
      i += 1
    }
    sc.stop()

  }

  def computeRmse(model: MatrixFactorizationModel, validation: RDD[Rating], numValidation: Long): Double = {
    val predictions: RDD[Rating] = model.predict(validation.map(x => (x.user, x.product)))
    val predictionsAndRating = predictions.map(x => ((x.user, x.product), x.rating))
      .join(validation.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRating.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / numValidation)
  }

  def loadRatings(path: String): Seq[Rating] = {
    val line = Source.fromFile(path).getLines()
    val ratings = line.map {
      line =>
        val fields = line.split("::")
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.filter(_.rating > 0.0)
    if (ratings.isEmpty) {
      sys.error("no rationgs provided.")
    } else {
      ratings.toSeq
    }
  }

}
