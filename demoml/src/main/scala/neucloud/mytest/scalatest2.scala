package neucloud.mytest

/**
  * Created by Administrator on 2016/9/23 0023.
  */
object scalatest2 {

  def main(args: Array[String]): Unit = {

    scalatest.decorate("wew")

    val bb : Null = null

  }
  def pany(x:AnyRef) = {
    val s = x.getClass.getName
    println(s)

  }
}
