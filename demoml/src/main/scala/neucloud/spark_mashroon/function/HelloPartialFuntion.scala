package neucloud.spark_mashroon.function

import java.io.IOException


/**
  * Created by Administrator on 2016/10/18 0018.
  */
object HelloPartialFuntion {
  def main(args: Array[String]): Unit = {
    try{

      1/0
    } catch {
      case ioExcept : IOException => println(ioExcept.toString)
      case ill : IllegalArgumentException => println("jary"+ill.toString)
      case aa : ArithmeticException => print("jary"+aa.toString)

    }

    val sample = 1 to 10
    val isEven : PartialFunction[Int,String] = {
      case x if x%2 == 0 => x+" is even"
    }

    val isOdd: PartialFunction[Int, String] = {
      case x  => x+" is odd"
    }
    val numbers = sample map (isEven orElse isOdd) foreach println



  }


}
