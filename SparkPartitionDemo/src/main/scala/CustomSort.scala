import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2016/5/18.
 */

// 第二种隐式转化
object OrderContext {

  implicit object GirlOrdering extends Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
      if (x.faceValue > y.faceValue) 1
      else if (x.faceValue == y.faceValue) {
        if (x.age > y.age) -1 else 1
      } else {
        -1
      }
    }
  }

}

object CustomSort {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PartitionerDmeo").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(("yousss", 90, 28, 1), ("angellabby", 90, 27, 2), ("xxxx", 98, 27, 3)))

    import OrderContext._
    val rdd2 = rdd1.sortBy(x => Girl(x._2, x._3), false)
    println(rdd2.collect().toBuffer)
    sc.stop()
  }

}

// 第一种方式
//case class Girl(val faceValue: Int, val age: Int) extends Ordered[Girl] with Serializable {
//  override def compare(that: Girl): Int = {
//    if (this.faceValue == that.faceValue) {
//      that.age - this.age
//    } else {
//      this.faceValue - that.faceValue
//    }
//  }
//}

case class Girl(val faceValue: Int, val age: Int) extends Serializable