/**
  * Created by mahuichao on 16/8/29.
  */
object MyTest {
  def main(args: Array[String]): Unit = {
    val s = "hello world my love cat"
    val r ="""my""".r
    val result = r.findAllIn(s)
    println(result.hasNext)


  }
}
