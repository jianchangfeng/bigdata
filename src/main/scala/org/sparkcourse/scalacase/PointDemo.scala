package org.sparkcourse.scalacase

// class
class Point(val xc: Int, val yc: Int) {
  var x: Int = xc
  var y: Int = yc

  def move(dx: Int, dy: Int): Unit = {
    x = x + dx
    y = y + dy
    if(x > 1)
      println ("Point x location : " + x);
    println ("Point y location : " + y);
  }
}

// 特质 (Traits) 用于在类 (Class)之间共享程序接口 (Interface)和字段 (Fields)。 它们类似于Java 8的接口。 类和对象 (Objects)可以扩展特质，但是特质不能被实例化，因此特质没有参数。
trait Move {
  def move(dx: Int, dy: Int) : Unit
}

class PointWithTrait(xc: Int, yc: Int) extends Move {
  var x: Int = xc
  var y: Int = yc

  def move(dx: Int, dy: Int): Unit = {
    x = x + dx
    y = y + dy
    if(x > 1)
      println ("Point x location : " + x);
    println ("Point y location : " + y);
    PointWithTrait.testMethod(PointWithTrait.testVar)
  }
}
// 伴生对象
// 如果有同样一个类与该object名字一样，则称该object为该类的伴生对象，相对应，该类为object的伴生类。如果一个类有它的伴生对象，这个类就可通过object的名字访问到所有成员，但object不在该类的作用范围。
// object对象为静态常量、静态变量区域，可以直接调用，共享全局变量很有意义，伴生对象方便类的构建，可做为当前类的静态方法、成员的集合。
object PointWithTrait {
  var testVar = 100
  def testMethod(a: Int): Unit = {
    println("print var %s".format(a))
  }
}

// Singleton Objects
// Scala is more object-oriented than Java because in Scala, we cannot have static members.
// Instead, Scala has singleton objects.
// A singleton is a class that can have only one instance, i.e., Object.
// You create singleton using the keyword object instead of class keyword.
// Since you can't instantiate a singleton object, you can't pass parameters to the primary constructor.
// You already have seen all the examples using singleton objects where you called Scala's main method.
// 单例对象
object PointDemo {
  def test(): Unit = {
    println("test method from PointDemo")
  }

  def main(args: Array[String]) {
    // new an object
    val pt = new Point(10, 20);

    // Move to a new location
    pt.move(10, 10);

    // new an object
    val pt2 = new PointWithTrait(10, 20);

    // Move to a new location
    pt2.move(10, 10);
    test()
  }
}
