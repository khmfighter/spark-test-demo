package neucloud.spark_mashroon

/**
  * Created by Administrator on 2016/10/25 0025.
  */
object HelloAny {
  def main(args: Array[String]): Unit = {
    val succ = (x : Int) => x +1
    val anonfun1 = new Function[Int,Int] {
      override def apply(x : Int ) :Int = x+1
    }

    assert(succ(0) == anonfun1(0))
  }

}
