package neucloud.spark_mashroon.patten_match

/**
  * Created by Administrator on 2016/10/11 0011.
  */
class DataFramew
private case class CpomputF(name : String ,popu : Boolean) extends DataFramew
private case class StorageF(name : String ,popu : Boolean) extends DataFramew
object patten {

  def main(args: Array[String]): Unit = {
    getS("spark3")
    getMatchCollection(Array("s"))
    getBigData(CpomputF("spark",true))
    getValue("spark",Map("spark " -> "ssss"))
    Array("dd",2)
    List("i am spi dd d s").flatMap(_.split(" ")).map(a => (a,1))
  }

  def getS(name: String): Unit = {
    name match {
      case "spark" => println("15")
      case "hdoop" => println("10")

      case _ => println("4")
      case _nn => println(2)
    }
  }

  def getMatchCollection(msg: Array[String]): Unit = {
    msg match {
      case Array("s") => println(msg(0))
      case Array("j") => println(msg)
      case Array("m") => println(msg)
    }
  }

  def getBigData(df : DataFramew): Unit ={

    df match {
      case  CpomputF(name,p) => println("name :" + name + "ispopu =" + p)
      case Stream(n,p) => println("name :" + n + "ispopu =" + p)
    }
  }


  def getValue(key : String ,m : Map[String,String]){

    m.get(key) match {
      case Some(v) => println(v)
      case None => println("null")
    }
  }
}
