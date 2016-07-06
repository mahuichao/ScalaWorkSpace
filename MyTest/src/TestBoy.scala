/**
 * Created by Administrator on 2016/5/15.
 */
class TestBoy {

}

object TestBoy {
  def main(args: Array[String]) {
    val b1 = new Boy("laoduan", 99)
    val b2 = new Boy("laozhao", 999)

    val arr = Array(b1, b2)
    var arr1 = arr.sortBy(x => x).reverse
    for (s <- arr1)
      println(s.name)

  }
}
