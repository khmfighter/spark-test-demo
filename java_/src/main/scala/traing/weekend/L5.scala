package traing.weekend

import java.io.{File, FileInputStream, PrintWriter}
import scala.io.Source

/**
  * Created by jaryzhen on 2016/11/17 0017.
  */
object L5 {

  def main(args: Array[String]): Unit = {

  }

  def stuff(): Unit = {
    val file = Source.fromFile("data/stuff2.csv", "UTF-8")
    val getl = file.getLines().map(sp)
  }

  /*
  案例1 备件消耗-样例数据
1、读取文件
2、按照类别汇总损坏个数和总金额、平均金额。
3、把统计结果写入文件
案例2 备件消耗-样例数据
1、按照类别、损坏个数排序。
2、对总金额大于“XXX”的进行筛选比较，
3、将符合要求的对应的物料代码和损坏个数数据写入结果文件
   */
  def stuff2(): Unit = {
    //val cmap = new util.HashMap[String, Double]
    var cmap = Map.empty[String, Double]

    //"ISO-8859-1"
    val file = Source.fromFile("data/stuff2.csv", "ISO-8859-1")
    val b = file.getLines().map(_.toString)
    //b.foreach(println)
    val m = b.map(_.split(","))
/*
    val wc = m.map(a => {
      (a(2), a(4).toDouble)
    }).map(word => {
        if (cmap.contains(word._1))
          cmap += (word._1 -> (cmap(word._1) + word._2))
        else
          cmap += (word._1 -> word._2)
      }).toList

    for ((k, v) <- cmap) {
      //println(k + "....." + v)
    }*/

    var soredMap  = Map.empty[String,String]
    val sort = m.map(a =>{
      (a(0),a(1),a(2),a(3))
    }).map(a => {
      soredMap += (a._4 -> a.toString)
    }).toList
    for ((k,v) <- soredMap) {
      k.sorted
      println(""+k.sorted)
    }

    //    if (map.contains(word))
    //      map += (word -> (map(word) + 1))
    //    else
    //      map += (word -> 1)

    def arry(array: Array[String]): Unit = {
      println(array.length)
    }
    //m2.foreach(print)

    //.filter(_.length>5).map(a => {(a(2),a(4))}).foreach(print)
    //file.foreach(print)
    /*    for (line <- file.getLines) {
          val cols = line.split(",").map(_.trim)
          // do whatever you want with the columns here
          println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
        }*/
    file.close
  }

  def sp(string: String) = {
    println(string)
  }

  def cout(s: Unit): Boolean = {
    true
  }
}

