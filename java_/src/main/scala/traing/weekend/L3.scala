package traing.weekend

/**
  * Created by Administrator on 2016/11/16 0016.
  */
object L3 {

  /* 方法是对对象进行操作，有明确的返回值，而函数是一条或几条特定任务的代码快，返回结果是任意类型的 ，形如：f(x) = x+1 . 在scala中函数是一等公民，这意味着函数可以像变量一样被传递，被赋值，同时也可以赋值给变量，变量也可以赋值给函数。之所以这样是因为函数背后是类和对象，也就是说在运行时函数其实是一个变量 ，这个由scalac 编译器来帮我们生成。
     意义：
         1. 可以天然序列化和反序列化
         2. 因为序列化 所以可以在分布式系统上传递

    闭包的内幕：Scala的函数背后是类和对象，所以，Scala的参数都作为了对象的成员！！！！！
   */
  def main(args: Array[String]): Unit = {
    //println(hello("scala"));
   // val sums = sum(1,2,3,4)
   // println(sums)
    //val sums2 = sum(1 to 10 : _*)
    val a = map(hello _)

    val bib = hiBigData _
    bib("fa")

    // 匿名函数
    val f1 = (name :String ) => hiBigData _
    f1("jjjj")("jj")
    f2("jjjj")("jj")//这就是可里化

  }

  //函数
  def hello(name: Int) = {
    println("hello " + name)
    "dfafda"
  }

  def sum(numbs: Int*) = {
    var resu = 0
    for (n <- numbs) {
      resu += n
    }
    resu
  }

  //闭包
  def map (f : Int => String) = {
    val a = 222
    println(f(a))
  }

  //高级
  def hiBigData(t : String) ={
    println("hi  . . ... "+t )
  }
//
  def f2 = {(name :String ) => hiBigData _}
}
