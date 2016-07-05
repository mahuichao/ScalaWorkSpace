import RedisClient._
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

/**
 * Created by Administrator on 2016/6/14.
 */
object LogLoader {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("loadLog")
    val sc = new SparkContext(conf)
    val path = "hdfs://sun:9000/user/hdfs/2016*"
    val rdd = sc.textFile(path)
    val lines = rdd.map(x => {
      val m = x.split("\t", -1)
      if (m.length < 13) {
        ("", "", "", "")
      } else {
        val timeAll = m(2)
        val timeStr = timeAll.split(" ")
        val time = timeStr(2)
        val date = timeStr(0)
        // 时间、url、账号、目标IP、match_host
        (date + " " + time, m(3), m(4), m(9))
      }
    })

    // 过滤掉没用日志
    val line1 = lines.filter(x => !(x._2 == "" || x._2.contains(".js") || x._2.contains(".css") || x._2.matches("^[0-9]+") || x._2.matches("^/.*") || (!x._2.contains("/"))))
    val line2 = line1.filter(x => x._2.matches("[^/]*www\\.cnki\\.net/.*") || x._2.matches("[^/]*exam\\.pkulaw\\.cn/.*") || x._2.matches("[^/]*www\\.pkulaw\\.cn/.*") || x._2.matches("[^/]*g\\.wanfangdata\\.com\\.cn/.*") || x._2.matches("[^/]*www\\.nssd\\.org/.*"))
    line2.foreachPartition(partition => {
      val redis = pool.getResource
      partition.foreach(x => {
        val regex = """(.*?)/.*""".r
        val regex(match_host) = x._2
        val document = Map(
          "create_time" -> x._1,
          "url" -> x._2,
          "account" -> x._3,
          "ip" -> x._4,
          "match_host" -> match_host
        )
        implicit val formats = DefaultFormats
        val str = Json(DefaultFormats).write(document)
        redis.lpush(match_host, str)
      })
    }
    )

    sc.stop()

  }
}
