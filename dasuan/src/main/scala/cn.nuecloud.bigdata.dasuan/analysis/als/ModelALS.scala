package cn.nuecloud.bigdata.dasuan.analysis.als

import java.util

import cn.nuecloud.bigdata.utils.{DSLogging, SparkContextImpl}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import scala.collection.JavaConversions._

/**
  * Created by jary on 12/2/15.
  */
private[dasuan] object ModelALS {
  def main(args: Array[String]): Unit = {
    //
    val ui = new ModelALS
    val userRatingsData = "data/mllib/als/movielens/medium/sample_movielens_ratings.txt"//sample_movielens_ratings.txt ratings.dat
    val allratingsData = "data/mllib/als/movielens/medium/ratings.dat"
    val itemsData  = "data/mllib/als/movielens/medium/movies.dat"
    val sc = SparkContextImpl(this.getClass.getName)
    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)

    //val lsss = ui.tranALS(sc, userRatingsData, allratingsData, itemsData, ranks, lambdas, numIters)

    val userRatingsData2 :DataFrame =null
    val ratingsData2 :DataFrame =null
    val itemsData2 :DataFrame =null // = "data/mllib/als/movielens/medium/movies.da"
   // val lsss2 = ui.tranALS2(sc, userRatingsData2, ratingsData2, itemsData2, ranks, lambdas, numIters)

  }
}

private[dasuan] class ModelALS extends DSLogging {
//  def tranALS2(sc: SparkContext, userRatingsData: DataFrame, ratingsData: DataFrame, itemsData: DataFrame, ranks: List[Int], lambdas: List[Double], numIters: List[Int]) = {
//  }

  /**
    * @param sc SparkContext
    * @param userRatingsData  user评分数据
    * @param ratingsData   样本评分数据
    * @param itemsData  item 数据
    * @param jranks   模型隐形因子
    * @param jlambdas als 正则化参数
    * @param jnumIters 迭代次数
    */
  def tranALS(sc: SparkContext, userRatingsData : String, ratingsData : String, itemsData : String, jranks: util.List[Int], jlambdas : util.List[Double], jnumIters : util.List[Int]): util.ArrayList[String] = {
    val numPartitins = 1

    //装载样本评分数据，其中最后一列Timestamp取除10的余数作为key，Rating为值，即(Int，Rating)
    val ratingsRDD =ParseFileData.ratingDF(sc,ratingsData)

    //装载电影目录对照表(电影ID->电影标题)
    val moviesMap = ParseFileData.movieDF(sc,itemsData)

    val userRatingsRDD = ParseFileData.userDF(sc,userRatingsData)

    //统计样本中用户数量和电影数量以及用户对电影的评分数目
    val numRatings = ratingsRDD.count()
    val numMovies = ratingsRDD.map(_._2.product).distinct().count()
    val numUsers = ratingsRDD.map(_._2.user).distinct().count()

    loginfo_a("got " + numRatings + " rating from " + numUsers + " users on " + numMovies + " moviesMap.")

    //将样本评分表以key值切分成3个部分，分别用于训练 (60%，并加入用户评分), 校验 (20%), and 测试 (20%)
    val training = ratingsRDD.filter(x => x._1 < 6).values.union(userRatingsRDD).repartition(numPartitins)
    val validation = ratingsRDD.filter(x => x._1 >= 6 && x._1 < 8).values.repartition(numPartitins)
    val test = ratingsRDD.filter(x => x._1 >= 8).values

    val numTraining = training.count()
    val numValidation = validation.count()
    val numtest = test.count()
    loginfo_a("Traing:" + numTraining + " , validation:" + numValidation + " , test:" + numtest)

    //训练不同参数下的模型，并在校验集中验证，获取最佳参数下的模型
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambdas = -1.0
    var bestNumIter = -1
    val ranks = jranks.toList
    val lambdas = jlambdas.toList
    val numIters = jnumIters.toList

    for (rank <- ranks; lambda <- lambdas; numiter <- numIters) {
      val matrixFactorizationModel = ALS.train(training, rank, numiter, lambda)
      val validationRmse = computeRmse(matrixFactorizationModel, validation, numValidation)
      loginfo_a("RMSE (validation) = " + validationRmse + "for the matrixFactorizationModel trained with rank =" + rank + ",lanbda= " + lambda + ", and Numiter = " + numiter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(matrixFactorizationModel)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambdas = lambda
        bestNumIter = numiter
      }
    }
    //用最佳模型预测测试集的评分，并计算和实际评分之间的均方根误差（RMSE）
    val testRmse = computeRmse(bestModel.get, test, numtest)
    loginfo_a("the best model was trained with rank = " + bestRank + " and lamda = " + bestLambdas + ", and numiter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")

    val meanRating = training.union(validation).map(_.rating).mean()
    val baselineRmse = math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean())
    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    loginfo_a(" the best model improves the baselkine by " + "%1.2f".format(improvement) + "%.")

    val myRatedMovieIds = userRatingsRDD.map(_.product).toString()
    val candidates = sc.parallelize(moviesMap.keys.filter(!myRatedMovieIds.contains(_)).toSeq)

    //推荐前十部最感兴趣的电影，注意要剔除用户已经评分的电影
    val resultList = new  util.ArrayList[String]
    val recommendations = bestModel.get.predict(candidates.map((0, _)))
      .collect().sortBy(-_.rating).take(10)
    var i = 1
    loginfo_a("moveies recommemdation for u is :")
    recommendations.foreach { r =>
      loginfo_a("%2d".format(i) + ":" + moviesMap(r.product))
      resultList.add( "%2d".format(i) + ":" + moviesMap(r.product))
      i += 1
    }
    sc.stop()
    resultList
  }

  //校验集预测数据和实际数据之间的均方根误差
  def computeRmse(model: MatrixFactorizationModel, validation: RDD[Rating], numValidation: Long): Double = {
    val predictions: RDD[Rating] = model.predict(validation.map(x => (x.user, x.product)))
    val predictionsAndRating = predictions.map(x => ((x.user, x.product), x.rating))
      .join(validation.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRating.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / numValidation)
  }
}
private[als] object ParseDFData {
  def movieDF(sc:SparkContext ,dataFrame: DataFrame): Map[Int,String] ={
    val map  = dataFrame.toJSON.map{line =>
    val fileds = line.split("::")
    (fileds(0).toInt, fileds(1)) }.collect().toMap
    map
  }
  def ratingDF(sc:SparkContext ,dataFrame: DataFrame): RDD[(Long,Rating)] = ???
  def userDF(sc:SparkContext ,dataFrame: DataFrame): RDD[Rating] = ???
}
private[als] object ParseFileData {
  /**装载电影目录对照表(电影ID->电影标题)
    $ movies.dat
    MovieID::Title::Genres
    1::Toy Story (1995)::Animation|Children's|Comedy
    */
  def movieDF(sc:SparkContext ,string: String): Map[Int,String] ={
    val movies = sc.textFile(string).map {
      line => val fileds = line.split("::")
        (fileds(0).toInt, fileds(1))
    }.cache().collect().toMap
    movies
  }
  /**
  $ ratings.dat
    UserID::MovieID::Rating::Timestamp
    1::1193::5::978300760
    */
  def ratingDF(sc:SparkContext ,string: String): RDD[(Long,Rating)] ={
    val ratiing = sc.textFile(string).map({
      lines => val fields = lines.split("::")
        (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    })
    ratiing
  }
  /**
  $ users.dat
    UserID::Gender::Age::Occupation::Zip-code
    1::F::1::10::48067

    测试数据
    userRatings.txt
    UserID::MovieID::Rating::Timestamp
    前2个数据文件用于模型训练，userRatings.txt 数据文件用于预测模型
    */
  def userDF(sc:SparkContext ,path: String): RDD[Rating] = {
    val ratings = sc.textFile(path).map {
      line => val fields = line.split("::")
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.filter(_.rating > 0.0).repartition(1)
    ratings
  }
}
