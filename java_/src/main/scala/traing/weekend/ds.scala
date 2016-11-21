package traing.weekend

import java.io.File

import scala.io.Source

/**
  * Created by Administrator on 2016/11/19 0019.
  */
class ds {

}

class WorldCount {
  var map = Map.empty[String, Int]
  def scanDir(file: File) {
    file.listFiles().foreach { f => if (f.isFile()) readFile(f) }
  }
  def readFile(file: File) {
    val f = Source.fromFile(file,"ISO-8859-1")
    for (line <- f.getLines()) {
      for (word <- line.split("\t|,|\\\\|\\+|\\-|\\(|\\)|\\[|\\]|!|:|\\.|>|<|\\{|\\}|\\?|\\*| |\\/|\"")) {
        if (map.contains(word))
          map += (word -> (map(word) + 1))
        else
          map += (word -> 1)
      }
    }
  }
}
object WorldCount {
  def main(args: Array[String]): Unit = {
    val wordCount = new WorldCount()
    wordCount.scanDir(new File("data/stuff2.csv"))
    wordCount.map.foreach{x=>println("world:"+x._1+"  count:"+x._2)}
  }
}