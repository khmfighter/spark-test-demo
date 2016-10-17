package neucloud.spark_mashroon.trit

/**
  * Created by Administrator on 2016/9/27 0027.
  */
trait Logged {
  val  ttt = 88;
  def log(msg : String)//{println("trait Logger"+msg)} //这是一个抽象方法
  def infor(msg : String){log(" INFO :"+msg)}
  def warn(msg : String){log(" WARN  : "+msg)}
  def server(msg : String){log(" SERVER :"+msg)}
}
