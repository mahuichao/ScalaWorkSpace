/**
 * Created by Administrator on 2016/5/15.
 */
class Pair[T <: Comparable[T]] {
  def compares(first: T, second: T): T = {
    if (first.compareTo(second) > 0) first else second
  }
}

object Pair {
  def main(args: Array[String]) {
    val p = new Pair[String]
    println(p.compares("hadoop", "spark"))
  }
}
