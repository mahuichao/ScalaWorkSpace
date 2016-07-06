/**
 * Created by Administrator on 2016/5/15.
 */
class Boy(val name: String, val faceValue: Int) extends Comparable[Boy] {
  override def compareTo(o: Boy): Int = {
    this.faceValue - o.faceValue
  }
}
