package traing.weekend.wind

/**
  * Created by Administrator on 2016/11/16 0016.
  */
object L2 {

  def main2(args: Array[String]): Unit = {


  }

  def arry_(): Unit = {
    // 1.数组
    val arry = new Array[Int](10)
    arry(0) = 21;
    arry(109) = 22
    println(arry(0))

    var myList = Array(1.9, 2.9, 3.4, 3.5)

    // Print all the array elements
    for (x <- myList) {
      println(x)
    }

    // Summing all elements
    var total = 0.0;
    for (i <- 0 to (myList.length - 1)) {
      total += myList(i);
    }
    println("Total is " + total);

    // Finding the largest element
    var max = myList(0);
    for (i <- 1 to (myList.length - 1)) {
      if (myList(i) > max) max = myList(i);
    }
    println("Max is " + max);
  }

  def list_(): Unit = {
    /*
      * 列表的基本操作：
      * 上列出了所有的操作都可以体现在以下三个方法来讲：

      * 方法	描述
      * head	此方法返回的列表中的第一个元素。
      * tail	此方法返回一个由除了第一个元素外的所有元素的列表。
      * isEmpty	如果列表为空，此方法返回true，否则为false。
      */

    //创建统一列表：
    //可以使用List.fill()方法创建，包括相同的元素如下的零个或更多个拷贝的列表：
    val fruits = List.fill(3)("apples") // Repeats apples three times.
    println("fruit : " + fruits)
    val num = List.fill(10)(2) // Repeats 2, 10 times.
    println("num : " + num)


    //可以使用:::运算符或列表List.:::()方法或List.concat()方法来添加两个或多个列表
    val fruit1 = "apples" :: ("oranges" :: ("pears" :: Nil))
    val fruit2 = "mangoes" :: ("banana" :: Nil)

    // use two or more lists with ::: operator
    var fruit = fruit1 ::: fruit2
    println("fruit1 ::: fruit2 : " + fruit)

    // use two lists with Set.:::() method
    fruit = fruit1.:::(fruit2)
    println("fruit1.:::(fruit2) : " + fruit)

    // pass two or more lists as arguments
    fruit = List.concat(fruit1, fruit2)
    println("List.concat(fruit1, fruit2) : " + fruit)

    //可以使用List.reverse方法来扭转列表
    println("Before reverse fruit : " + fruit)
    println("After reverse fruit : " + fruit.reverse)
  }

  def set_(): Unit = {
    /*
      * 方法	描述
      * head	此方法返回集合的第一个元素。
      * tail	该方法返回集合由除第一个以外的所有元素。
      * isEmpty	如果设置为空，此方法返回true，否则为false。
      */
    var s0: Set[Int] = Set()

    // Set of integer type
    var s1: Set[Int] = Set(1, 3, 5, 7)
    //or
    var s2 = Set(1, 3, 5, 7)

    //可以使用++运算符或集。++()方法来连接两个或多个集，但同时增加了集它会删除重复的元素。以下是这个例子来连接两个集合：
    val fruit1 = Set("apples", "oranges", "pears")
    val fruit2 = Set("mangoes", "banana")

    // use two or more sets with ++ as operator
    var fruit = fruit1 ++ fruit2
    println("fruit1 ++ fruit2 : " + fruit)

    // use two sets with ++ as method
    fruit = fruit1.++(fruit2)
    println("fruit1.++(fruit2) : " + fruit)

    val num = Set(5, 6, 9, 20, 30, 45)

    // 查找集合中最大，最小的元素
    println("Min element in Set(5,6,9,20,30,45) : " + num.min)
    println("Max element in Set(5,6,9,20,30,45) : " + num.max)
  }

  def map_(): Unit = {
    /* 方法	描述
       keys	这个方法返回一个包含映射中的每个键的迭代。
     values	这个方法返回一个包含映射中的每个值的迭代。
     isEmpty	如果映射为空此方法返回true，否则为false。
     */
    var A: Map[Char, Int] = Map()
    // A map with keys and values.
    val colors = Map("red" -> "#FF0000", "azure" -> "#F0FFFF")

    //键值对添加到映射
    A += ('I' -> 1)
    A += ('J' -> 5)
    A += ('K' -> 10)
    A += ('L' -> 100)

    //打印
    colors.keys.foreach { i => print("Key = " + i)
      println(" Value = " + colors(i))
    }
    for (a <- colors) println(a)

    //检查映射中的键：
    if( colors.contains( "red" )){
      println("Red key exists with value :"  + colors("red"))
    }else{
      println("Red key does not exist")
    }
    if( colors.contains( "maroon" )){
      println("Maroon key exists with value :"  + colors("maroon"))
    }else{
      println("Maroon key does not exist")
    }
  }
  def tuple_(): Unit ={
    val t = (4,3,2,1)
    val t2 = Tuple3(2,"23",22)
    val sum = t._1 + t._2 + t._3 + t._4
    println( "Sum of elements: "  + sum )
  }
}

object LazyOps {

  def init(): String = {
    println("call init()")
    return ""
  }

  def main(args: Array[String]) {
    lazy val property = init(); //使用lazy修饰
    println("after init()")
    println(property)
    println(property)
  }

}

