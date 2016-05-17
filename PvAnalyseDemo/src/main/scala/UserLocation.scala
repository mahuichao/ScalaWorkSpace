import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2016/5/17.
 */
object UserLocation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("UserLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D://bs_log").map(line => {
      val field = line.split(",")
      val eventType = field(3)
      val time = field(1)
      val timeLong = if (eventType == "1") -time.toLong else time.toLong
      (field(0) + "_" + field(2), timeLong)
    })

    val rdd1 = lines.groupBy(_._1).mapValues(_.foldLeft(0L)(_ + _._2))
    val rdd2 = rdd1.map(t => {
      val m = t._1
      val mobile = m.split("_")(0)
      val lac = m.split("_")(1)
      val time = t._2
      (m, lac, time)
    })
    val rdd3 = rdd2.groupBy(_._1)
    val rdd4 = rdd3.mapValues(it => {
      it.toList.sortBy(_._3).reverse.take(2)
    })
    sc.stop()
  }


}
