package neucloud.spark_mashroon.trit

import scala.collection.mutable.HashMap

/**
  * Created by Administrator on 2016/9/27 0027.
  */
class SavingAccount extends {
  val filename: String = "gnore"
} with Account with Filelogger {

  val iptest = 0.0
  //override val balance = 2
  def drawing(amount : Double): Unit ={

    if(amount > balance) {
      log("Insu funds")
      println(balance  +"" )
    }
    else balance -= amount
  }

}

object SavingAccount {
  def main(args: Array[String]): Unit = {
    val acct1 = new  SavingAccount
    acct1.drawing(2)

    println(acct1.ttt)
    val souce  = new HashMap[String,Int]
    souce("bob ") = 988

    val ss = mu(3)
    println(index(1,2))

  }
  def mu(x:Int) = (y :Int) => x*y
  def index(l:Int, l2:Int) :Int ={
    if(l>l2){
      return l
    }else{
      return l2
    }
    333
  }
}