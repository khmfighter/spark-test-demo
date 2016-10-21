package neucloud.spark_mashroon

import java.io._

/**
  * Created by Administrator on 2016/10/20 0020.
  */
class DTSpark(val name:String) extends Serializable
object HelloDeSearialiyze extends App{

  val dTSpark = new DTSpark("spark")
//基于内存（对象流）的序列化和反序列化
  def serialize[T](o:T): Array[Byte]={
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o) //内存
    oos.close()

    bos.toByteArray
  }

  println(new String (serialize(dTSpark)),"UTF-8")
  def deserialize[T](byrte : Array[Byte]) :T ={
    val bis = new ByteArrayInputStream(byrte)
    val ois = new ObjectInputStream(bis)
    ois.readObject().asInstanceOf[T]
  }
  println(deserialize[DTSpark](serialize(dTSpark)).name)

  //基于文件的 序列化和反序列化
  def serialize_file[T](o : T)={
    val bos = new FileOutputStream("sss")
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
  }
  def dserialize_file[T](s: String) : T ={
    val bos = new FileInputStream(s)
    val oos = new ObjectInputStream(bos)
    oos.readObject().asInstanceOf[T]
  }
  serialize_file(dTSpark)
  println(dserialize_file[DTSpark]("sss").name)
}
