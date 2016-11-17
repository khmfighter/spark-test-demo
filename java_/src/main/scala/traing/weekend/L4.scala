package traing.weekend

/**
  * Created by JaryZhen on 2016/11/16 0016.
  */
object L4 {

}
abstract class 动物 {
  val 能动的 = true
  val 脚 : Int
  val 以光合作用来生存 = false
}
trait 身体是健全的{
  val 嘴 = 1
  val 眼睛 = 2
  val 耳朵 = 2
  val 指头 = 10
}
trait 自带特效的{
  def 特效1()
  def 特效2()
  def 特效3()
}
abstract class 人 extends 动物 with 身体是健全的  {
  def 说话(给听的人: String)
  def 唱歌(歌名 : String)
}
trait 满18岁 {
  def 能谈恋爱(目标 :Int)
  def 乱谈恋爱(目标1 :Int ,目标2 :Int,目标3 :Int)
  def 不能叫做谈恋爱(目标x :Int*)
}
class 程序员 extends 人 with 满18岁{
  override def 说话(给听的人: String) = ???
  override def 唱歌(歌名: String) = ???
  override val 脚: Int = _

  override def 能谈恋爱(目标: Int) = ???
  override def 乱谈恋爱(目标1: Int, 目标2: Int, 目标3: Int) = ???
  override def 不能叫做谈恋爱(目标x: Int*) = ???
}