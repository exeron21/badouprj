package scala_demo

object ImplicitFun {
  def fun1[k,v] {
  }


//  def run[T](t:T) = println(t)

  def run[T <: Dog] (t:T) = println(t.name_ * 2)

  def main(args: Array[String]): Unit = {
    run(new Dog("haha"))
  }
}

class Dog (name:String) {
  val name_ = name
}

class Jing8(name:String) extends Dog(name:String) {

}
