package test

object CaseClass {
  case class Person (id:Int, name:String) {
    def this() = this(0," ")
  }

}
