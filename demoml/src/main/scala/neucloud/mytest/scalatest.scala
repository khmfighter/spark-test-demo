package neucloud.mytest

/**
  * Created by Administrator on 2016/9/21 0021.
  */
object scalatest {

  def main(args: Array[String]): Unit = {

  /*  for (i <- 1 to 3; j <- 1.to(3) if i != j) print(i + j + ",")

    val a = for (i <- 1 to 10) yield "\n" + (i, i + 1, i + 2)
    // println(a)
*/
    println(decorate("da", "["))
  }

  def decorate(s: String, l: String = "{", r: String = "}"): String = {

    println(l + s + r)
    l + s + r
  }
}
