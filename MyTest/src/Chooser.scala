/**
 * Created by Administrator on 2016/5/15.
 */

import MyPref._

//class Chooser[T <% Ordered[T]] {
//  def choose(first: T, second: T): T = {
//    if (first > second) first else second
//  }
//
//}

class Chooser[T: Ordering] {
  def choose(first: T, second: T): T = {
    val ord = implicitly[Ordering[T]]
    if (ord.gt(first, second)) {
      first
    }
    else
      second
  }
}

object Chooser {
  def main(args: Array[String]) {
    val c = new Chooser[Girl]
    val g1 = new Girl("xinhua", 9999)
    val g2 = new Girl("zeyu", 8888)
    val d = c.choose(g1, g2)
    println(d.name)
  }
}