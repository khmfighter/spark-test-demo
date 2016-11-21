package traing.weekend.wind

/**
  * Created by Administrator on 2016/11/19 0019.
  */
object MyObjesct {
  def main(args: Array[String]): Unit = {
  }
}

abstract class 动物 {
  val 能动的 = true
  val 脚 : Int
  val 以光合作用来生存 = false
}
trait 身体是健全的{
  val b= 10
  val 眼睛 = 2
  val 耳朵 = 2
  val 指头 = 10
}
abstract class 人 extends 动物 {
  def 说话(给听的人: String)
  def 唱歌(歌名 : String)
}
trait 满18岁 {
  val b =2
  def 能谈恋爱(目标 :Int)
  def 乱谈恋爱(目标1 :Int ,目标2 :Int,目标3 :Int)
  def 不能叫做谈恋爱(目标x :Int*)
}

class 程序员 extends 人 with 满18岁 with 身体是健全的{
  override def 说话(给听的人: String) = ???
  override def 唱歌(歌名: String) = ???
  override val 脚: Int = 2
  override def 能谈恋爱(目标: Int) = ???
  override def 乱谈恋爱(目标1: Int, 目标2: Int, 目标3: Int) = {
    ///hjjjnkjnkjnjknkjnj
  }
 // override def 不能叫做谈恋爱(目标x: Int*) = {

  override def 不能叫做谈恋爱(目标x: Int*) = {

    println(眼睛)
}
}

