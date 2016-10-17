package neucloud.spark_mashroon

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016/10/8 0008.
  */
class SparkContextImpl(val name: String) extends {

  @transient override val conf = SparkContextImpl.confs.setAppName(name)
}
  with SparkContext(conf) {
  //config("spark.sql.warehouse.dir","file:///")
  //val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[10]").set("spark.sql.warehouse.dir", "file:///")
  //val sc = new SparkContext(conf)
}

object SparkContextImpl {
  //节省空间 全局性 唯一性 公用的 工厂类 vip：1.实验性的功能 2.将来要废弃掉的方法（便于架构和版本迭代）亡灵军团的召唤（scala隐式转换）

  def apply(name: String): SparkContext = {
    val sc: SparkContext = new SparkContextImpl(name)
    sc
  }
  //从配置文件读取
  @transient val masterurl = "local[2]"

  @transient val confs = new SparkConf().setMaster(SparkContextImpl.masterurl).set("spark.sql.warehouse.dir", "file:///")

  def main(args: Array[String]): Unit = {
    SparkContextImpl("dafa")
  }
}
