package neucloud.spark_mashroon.trit

/**
  * Created by Administrator on 2016/9/27 0027.
  */
trait ConsoleTimeLogged  extends Logged with Serializable{
   abstract override def log(msg: String) {
      super.log(new java.util.Date()+"" + msg)
   }
}
