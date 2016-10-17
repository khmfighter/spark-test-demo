package neucloud.spark_mashroon.function

/**
  * Created by Administrator on 2016/10/9 0009.
  *
  * 所以 函数可以作为参数传递给函数，这及大地简化了编程的语法：
  *    1.以前java的方式是new出一个接口实例，并且在接口实例的回调方法callback中来实现业务逻辑，现在是直接吧回调方法callback传递给我们的函数，且在函数中直接使用，这毫无疑问简化了代码的编写，提高了开发效率：
  *    2.这种方式非常方便编写负责业务逻辑和控制逻辑，对于图计算和机器学习至关重要，
  *    函数作为函数的参数传递的编程方式是称之为高阶函数的编程方式，在spark的源码和应用程序开发中至少60%都是这种代码，务必一定掌握
  *    4.函数式编程的非常强大之一在于方法的返回值可以是函数，当方法的返回值是函数时，这是就是闭包
  *      闭包的内幕：Scala的函数背后是类和对象，所以，Scala的参数都作为了对象的成员！！！！！
  *
  *
  */
object funtions {

  def main(args: Array[String]): Unit = {

    val bib = hiBigData _
        bib("fa")
    val f = (name :String ) =>{
      println("hi  . . ... "+ name )
      88
    }
    f("jary")
    getName(f,"scala")

    val multiple  = Array(1 to 10 : _*).map((item)=> item*2).foreach(println)
    function1("affda")

    val f2 = (name :String ) => hiBigData _
    f2("jjjj")("jj")//这就是可里化
    val 迭代 = f2("asd")
    迭代("hahahha ")

  }
  def function1  = (name :String ) => println("hi  . . ... "+ name )
  def function2  = (name :String ) => println("hi  . . ... "+ name )
  def getName(func : (String) => Int , name:String): Unit ={
    func(name)
  }
  def hiBigData(t : String): Unit ={
    println("hi  . . ... "+t )
  }

}
