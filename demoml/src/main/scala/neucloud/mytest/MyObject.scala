package neucloud.mytest

/**
  * Created by Administrator on 2016/9/28 0028.
  */
class MyObject() {
  val my1=11
  def myfu(s:String) = {
    println(s)

  }
}
object MyObject{
  def apply(): MyObject = new MyObject()
  val my2=22
  def myfuo(s:String) = println(s)

}