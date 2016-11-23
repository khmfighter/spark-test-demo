package cn.nuecloud.bigdata.utils



import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Jary on 2016/10/8 0008.
  */
class SparkContextImpl(val name : String) extends {

  @transient override val conf = SparkContextImpl.confs.setAppName(name)}
  with SparkContext(conf){
}

object SparkContextImpl{
  def apply(name : String): SparkContext = {
    Spark_Log()
    val sc: SparkContext = new SparkContextImpl(name)
    sc
  }

  @transient val masterurl = ConfReader.getString(SparkTag.SPARK_MASTER_LOCAL)
  @transient val confs = new SparkConf().setMaster(SparkContextImpl.masterurl).set("spark.sql.warehouse.dir", "file:///")
  def main(args: Array[String]): Unit = {
    SparkContextImpl("dafa")
  }
}