package neucloud.spark_mashroon

import scala.actors.Actor

/**
  * Created by Administrator on 2016/10/14 0014.
  */

object HelloActor {
  def main(args: Array[String]): Unit = {
    val helloActor = new HelloActor
    val helloActor2 = new HelloActor
    helloActor.start()
    helloActor2.start()
    val count = 0
    while (true){
      helloActor ! "wwww"
      helloActor2 ! "2222222222"
    }
  }

}

class HelloActor extends Actor {
  override def act() = {
    while (true) {
      receive { case c: String => println("message"+c)
      }
    }
  }
}
