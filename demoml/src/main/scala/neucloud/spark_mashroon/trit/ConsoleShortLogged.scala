package neucloud.spark_mashroon.trit

/**
  * Created by Administrator on 2016/9/27 0027.
  */
trait ConsoleShortLogged  extends Logged with Serializable{
   val maxLength :Int
   abstract override def log(msg: String): Unit ={

      super.log(if (msg.length <= maxLength ) msg else msg.substring(0,maxLength-3)+"...")
   }
}
