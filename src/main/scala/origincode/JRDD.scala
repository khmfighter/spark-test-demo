package origincode


import scala.reflect.ClassTag

/**
 * Created by spark on 1/19/16.
 */
abstract class JRDD[T : ClassTag]extends Serializable{

  def foreach(f:T => Unit): Unit = withScope{
    val sc.ru
  }

}
