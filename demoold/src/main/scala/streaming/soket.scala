package streaming

import java.io.PrintWriter
import java.net.ServerSocket

import org.apache.log4j.{Level, Logger}

import scala.io.Source

/**
 * Created by spark on 11/30/15.
 */
//send data by ip and port to other receiver
object Soket {

  def index(leng:Int) = {
    import java.util.Random
    val rdm = new Random()
    rdm.nextInt(leng)
  }

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val filename = "/home/spark/Desktop/data/people.txt"
    val lines = Source.fromFile(filename).getLines().toList
    val filerow = lines.length
    println("filerow"+filerow)
    val listener = new ServerSocket(9999)
    println("listener:"+listener)
    while(true){
      val  socket = listener.accept()
      new Thread(){
        override  def run ={
          println("got client connection from:"+socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream,true)
          while(true){
            Thread.sleep(2000)
            val content = lines(index(filerow))
            println(content)
            out.write(content+"\n")
            out.flush()
          }
          socket.close()
        }
      }.start()
      println("start():")
    }

  }
}

/**
 * filerow3
listener:ServerSocket[addr=0.0.0.0/0.0.0.0,localport=9999]
socket:


socket:Socket[addr=/192.168.137.100,port=57885,localport=9999]
socket:Socket[addr=/192.168.137.100,port=57885,localport=9999]
start():
socket:
got client connection from:/192.168.137.100
Michael, 29
Michael, 29
Andy, 30
Andy, 30
Andy, 30
Andy, 30
Andy, 30
Andy, 30
Justin, 19
Andy, 30
Justin, 19
Justin, 19
Andy, 30
 */

