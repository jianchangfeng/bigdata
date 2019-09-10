package org.sparkcourse.scalacase

object DemoScala {
  def varValExample(): Unit = {
    val a: Int = 10
    var b: Int = 10
    b += b + a
    println("b is :" + b)
  }

  def collectionAndAPI(): Unit = {
    // https://codeburst.io/a-beginner-friendly-intro-to-functional-programming-4f69aa109569
    val arr = Array(1,2,3)
    var index: Int = 0
    for(item <- arr) {
      arr(index) = item + 1
      println(arr(index))
      index += 1
    }

    var result = arr.map(1 + _)
    result = arr.map((x: Int) => {
      1 + x
    })
    arr.foreach(item => println(item))
    result.foreach(item => println(item))
    arr.filter(_ > 2).foreach(println(_))
    println(arr.reduce(_ + _))
  }

  def controlCase(): Unit = {
    // if else
    val x: Int = 30
    if (x < 20) {
      println("This is if statement")
    }
    else {
      println("This is else statement")
    }

    // multi case
    var caseIndex = 2
    val month = caseIndex match {
      case 1  => "January"
      case 2  => "February"
      case _  => "Invalid month"  // the default, catch-all
    }

    caseIndex match {
      case 1  => println("January")
      case 2  => println("February")
      case _  => println("Invalid month")  // the default, catch-all
    }

    // for loop
    val arr = Array(1,2,3)
    var index: Int = 0
    for(item <- arr) { // for (item <- 1 to 3)
      arr(index) = item + 1
      println(arr(index))
      index += 1
    }

    // while
    var i= 2
    while (i > 0){
      println(i)
      i -= 1
    }
  }

  def funcAddInt( a:Int, b:Int ) : Int = {
    var sum:Int = 0
    sum = a + b
    return sum
  }

  def funcAndClosures(): Unit = {
    println(funcAddInt(1, 2))
    // closure is a function, whose return value depends on the value of one or more variables declared outside this function.
    var factor = 2
    val addClosures = (i:Int) => i + factor
    println(addClosures(1))
  }

  def main(args: Array[String]): Unit = {
    funcAndClosures
  }

}
