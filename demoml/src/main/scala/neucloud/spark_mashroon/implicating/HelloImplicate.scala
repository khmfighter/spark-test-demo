package neucloud.spark_mashroon.implicating

/**
  * Created by Administrator on 2016/10/13 0013.
  */
class Man (){
  val name : String = null
  def y = println("man")
}
/*  z在 onject 中 定义影视函数
object Man{
  implicit  def toSB(man: Man) = new SB(man.name)
}
*/
class SB(){
  def sb2 = println("sb will to sky ")
}
object  implictsss{
  implicit def toSB(man: Man) = new SB()
}
object HelloImplicate {
  //1: implicit  def toSB(man: Man) = new SB() 银饰函数 定义在上下文代码

  def main(args: Array[String]): Unit = {
    // 2:
    import implictsss._
    val man = new Man()
    man.y
    man.sb2

    //talkSB("jm")("zhang san")
    implicit val string : String = "Li Si"
    talkSB("JM")
  }
  //3 隐士参数
  def talkSB(name: String)(implicit sb : String) =println("name: " + name + "talking " + sb)
}
