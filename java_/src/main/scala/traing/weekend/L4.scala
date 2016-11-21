package traing.weekend

import java.io.{File, FileInputStream, FileOutputStream, PrintWriter}

import scala.io.Source

/**
  * Created by JaryZhen on 2016/11/16 0016.
  */
object L4 {
}

/*
流是一组有顺序的，有起点和终点的字节集合，是对数据传输的总称或抽象。
即数据在两设备间的传输称为流，流的本质是数据传输

根据处理数据类型的不同分为：字符流和字节流
根据数据流向不同分为：输入流和输出流
 */
object File_In {
  def main(args: Array[String]): Unit = {
    in0()
  }

  def in0(): Unit = {
    /*
    读取二进制文件 (java)
     */
    val bytefile = new File("data/scala.txt")
    val in = new FileInputStream(bytefile)
    val byte = new Array[Byte](bytefile.length().toInt)
    in.read(byte)
    in.close()
  }
  /*
  读取非文件源的  URL
 */
  def in1(): Unit = {
    val url = Source.fromURL("https://www.baidu.com/", "UTF-8").foreach(print)
  }

  /*
  从屏幕读取一行：
   */
  def in2(): Unit = {
    print("Please enter your input : ")
    val line = Console.readLine
    println("Thanks, you just typed: " + line)
  }

  /*
   读取文件
    */
  def in3(): Unit = {
    //Source.fromFile("data/scala.txt", "UTF-8").foreach(print)
    Source.fromFile("data/scala.txt").foreach(print)

  }
}

object File_Out {
  /*字符流
    *写入数据
   */
  def out1(args: Array[String]) {
    /*写文件
     */
    val out = new PrintWriter("data/scala.txt")
    for (i <- 1 to 10) {
      out.print(i)
      out.write("hh")
    }
    out.close()
  }
  /*
  * 字节流
  * 向文件中一个字节一个字节的写入字符串
  * */
  def out1(s: String) {
    val out = new FileOutputStream(new File("out1.txt"), true);
    //true表示追加模式
    val str = "Hello World！！";
    val b = str.getBytes();
    for (i1 <- 0 to b.length) {
      out.write(b(i1))
    }
    out.close();
  }
}
