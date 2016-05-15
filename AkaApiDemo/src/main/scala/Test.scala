/**
 * Created by Administrator on 2016/5/14.
 */
class Test {

}

object Context {
  implicit val aaaaa = "laozhao"
  implicit val i = 1.toInt
}

object ImplicitValue {

  import Context.i

  def sayHi()(implicit name: Int = 123): Unit = {

    println(s"hi~ $name")
  }

  def main(args: Array[String]) {

    println(1 to 10)

    sayHi()
  }

}