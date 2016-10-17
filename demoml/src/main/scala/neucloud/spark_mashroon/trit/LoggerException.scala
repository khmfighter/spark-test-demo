package neucloud.spark_mashroon.trit

/**
  * Created by Administrator on 2016/9/27 0027.
  */
trait LoggerException  extends Logged{
  this : Exception => {def log2{log(getMessage())}}
}
