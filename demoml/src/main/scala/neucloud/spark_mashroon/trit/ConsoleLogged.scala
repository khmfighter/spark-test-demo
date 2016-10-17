package neucloud.spark_mashroon.trit

/**
  * Created by Administrator on 2016/9/27 0027.
  */
trait ConsoleLogged  extends Logged with Serializable{
   override def log(msg: String): Unit = println("1:"+msg)
}
