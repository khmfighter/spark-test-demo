package cn.nuecloud.bigdata.utils

import java.io.InputStream
import java.util.Properties

/**
  * 配置管理组件
  * <p>读取配置文件中类项，并可以根据需要将String类型的value转换成相应的类型<br>
  * Created by Jary on 2016/10/10 0010.
  */

class ConfReader{

}
object ConfReader extends DSLogging{

  val prop: Properties = new Properties()
  var in: InputStream = null
  try {
    in = ConfReader.getClass.getClassLoader.getResourceAsStream("dasuantag.properties")
    prop.load(in)
  } catch {
    case e: Exception => {
      e.printStackTrace();
      logError(e.toString)
    }
    case t : Throwable => logError(t.toString)
  }finally {
    in.close()
  }

  def getString(k : String) : String ={
    prop.getProperty(k)
  }
  def getInt(k : String): Int ={
    val v: String = prop.getProperty(k)
    try{
      return v.toInt
    } catch {
      case e : Exception => {
        e.printStackTrace();
        logError(e.toString)
      }
      case t : Throwable => logError(t.toString)
    }
    0
  }
}
