package neucloud.spark_mashroon.implicating

import scala.reflect.ClassTag

/**
  * Created by Administrator on 2016/10/12 0012.
  */

class Engineer
class Expert extends Engineer
class Meeting[T]

class Maximum[T : Ordering] (val x : T,val y : T){
  def bger(implicit ord : Ordering[T]): Unit ={
    if (ord.compare(x,y)>0) x else y
  }
}
object ScalaTypes {

  class Person(val name: String) {
    def talk(person: Person): Unit = {
      println(this.name + ":" + person.name)
    }
  }
  class Worker(name: String) extends Person(name)
  class Dog(val name: String)
  class Club[T <: Person](p1: T, p2: T) {
    def communicate = p1.talk(p2)
  }
  // view bound
  class Club2[T <% Person : ClassTag](p1: T, p2: T) {
    def communicate = p1.talk(p2)
  }

  def participateMeeting(meeting: Meeting[_]): Unit ={
    println("welcome "+meeting)
  }
  def main(args: Array[String]): Unit = {

    println(new Maximum(1,23).bger)

    val e = new Meeting[Engineer]
    participateMeeting(e)
    val expert = new Meeting[Expert]
    participateMeeting(expert)

  /*  val p = new Person("scala")
    val w = new Worker("spark")
    new Club[Person](p, w).communicate

    val dog = new Dog("dahuang")
    implicit def dog2P(dog: Dog) = new Person(dog.name)
    new Club2[Person](p, dog).communicate*/

  }
}
