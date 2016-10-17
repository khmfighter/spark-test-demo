package neucloud.spark_mashroon.trit

import java.io.PrintStream

/**
  * Created by Administrator on 2016/9/27 0027.
  */
trait Filelogger extends Logged{
  override val ttt: Int = 3434
  val filename : String
  val out = new PrintStream(filename)
  def log(msg : String) {out.println(msg) ; out.flush()}

}
