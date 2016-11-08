package neucloud.spark_mashroon.implicating

import java.io.File

/**
  * Created by Administrator on 2016/11/3 0003.
  */
class RicherFile(val f :File){

}
object RicherFile{
  def apply(f : File): RicherFile = new RicherFile(f)
}

object A{
  val a = "first"
  implicit def init2 (x:Int) = x.toString
  implicit def file2RicherFile( file : File) =   RicherFile(file)
}

object Imp extends App{

  def printContext(implicit s : String) = println(s)
  //implicit  val a="now"
  implicit val b = 2323
  import A._
  implicit val sss = "java"
  printContext




}
