package neucloud.spark_mashroon

/**
  * Created by Administrator on 2016/10/20 0020.
  */
case class Person1(name : String,age : Int)
class DTCoder(val name : String,val salary :Int)
object DTCoder{
  def apply( name: String, salary: Int)= {
    print("dtcoder apply mnethod }")
    new DTCoder(name,salary)
  }

  def unapply(arg: DTCoder) = {
    Some(arg.name,arg.salary)
  }
}
object HelloUnpply extends App{

  val person = Person1.apply("jary",18)
  val Person1(n,a) = person  ///天拉鲁   对象尽然赋值给了类
  println("name："+n+" age :"+a)

  person match {
    case Person1(nn,aa) => println("name:"+nn+"  age :"+ aa)
  }

  val dTCoder= DTCoder("hadoop",222)
  val DTCoder(name,sss)= dTCoder

  println("tools " + name+ "salary"+sss)
}
