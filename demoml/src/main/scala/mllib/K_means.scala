package mllib

import com.jary.Log_Sc
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}
;

/**
 * Created by spark on 12/2/15.
 */
object K_means {

  def main(args: Array[String]) {
    Log_Sc()
    val conf = new SparkConf().setAppName("kmeans").setMaster("local[2]")
    val sc  = new SparkContext(conf)

    val data = sc.textFile("/home/spark/Desktop/MyIntellij/SparkInAction/src/main/scala/demo.app.mllib/kmeans_data.txt",1)
    val parsedata = data.map(s => Vectors.dense(s.split(" ").map(_.toDouble)))

    val numcluster = 3
    val numiteraters = 20
    val model = KMeans.train(parsedata,numcluster,numiteraters)

    println("cluster centers")
    for (c <- model.clusterCenters ) println("..."+c.toString )

    val cost = model.computeCost(parsedata)

    println("within set sum of squared errors = "+ cost)

  //  println("vectors 0.2 0.2 0.2 is belong to clustders:"+ model.predict(Vectors.dense("0.2 0.1 0.2".split(" ").map(_.toDouble))))

    //only return result
//    val d = data.map(s => Vectors.dense(s.split(" ").map(_.toDouble)))
//    val result = model.predict(d)
//    result.saveAsTextFile("/home/spark/Desktop/MyIntellij/SparkInAction/src/main/scala/demo.app.mllib/result")

    val result2= data.map{
      line =>
        val lineVector = Vectors.dense(line.split(' ').map(_.toDouble))
        val prediction = model.predict(lineVector)
        line +" "+prediction
    }.saveAsTextFile("/home/spark/Desktop/MyIntellij/SparkInAction/src/main/scala/demo.app.mllib/result2")

    sc.stop()


  }
}
