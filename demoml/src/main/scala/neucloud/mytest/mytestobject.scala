package neucloud.mytest

/**
  * Created by Administrator on 2016/9/28 0028.
  */


object Mytestobject {

  def main(args: Array[String]): Unit = {

    val a =   MyObject()
    a.myfu("sdgfs")
  }
}

class Mytestobject extends MyObject{

  this.myfu("adfadf")
}

