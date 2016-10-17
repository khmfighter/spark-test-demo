package neucloud.spark_mashroon.trit

/**
  * Created by Administrator on 2016/9/27 0027.
  */
trait Console2Logged  extends Logged with Serializable{
   override def log(msg: String): Unit = println("2:"+msg)
}
