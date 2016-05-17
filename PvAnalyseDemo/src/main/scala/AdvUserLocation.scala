import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2016/5/17.
 */
object AdvUserLocation {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("UserLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd0 = sc.textFile("D://bs_log").map(line => {
      val field = line.split(",")
      val eventType = field(3)
      val time = field(1)
      val timeLong = if (eventType == "1") -time.toLong else time.toLong
      ((field(0), field(2)), timeLong)
    })

    val rdd1 = rdd0.reduceByKey(_ + _).map(t => {
      val mobile = t._1._1
      val lac = t._1._2
      val time = t._2
      (lac, (mobile, time))
    })

    val rdd2 = sc.textFile("D://loc_info.txt").map(line => {
      val f = line.split(",")
      // （基站，（经度，维度））
      (f(0), (f(1), f(2)))
    })
    val rdd3 = rdd1.join(rdd2).map(t => {
      val lac = t._1
      val mobile = t._2._1._1
      val time = t._2._1._2
      val x = t._2._2._1
      val y = t._2._2._2
      (mobile, lac, time, x, y)
    })
    val rdd4 = rdd3.groupBy(_._1)
    val rdd5 = rdd4.mapValues(it => {
      it.toList.sortBy(_._3).reverse.take(2)
    })
    rdd5.saveAsTextFile("D://result.txt")
  }
}
