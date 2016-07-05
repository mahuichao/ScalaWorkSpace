import org.apache.spark.{SparkContext, SparkConf}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json



/**
 * Created by Administrator on 2016/6/7.
 */

import RedisClient._

  object StartMyRedis {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("jedis")
    val sc = new SparkContext(conf)
    val path = "hdfs:/xxxx:8020/user/hdfs/logs/*"
    val rdd = sc.textFile(path)
    val lines = rdd.map(x => {
      val m = x.split(",", -1)
      (m(2), m(3), m(7), m(12), m(13))

    })
    val lines1 = lines.filter(x => x._3.contains("downloadFile_student.jsp") || x._3.contains("course_locate.jsp?course_id"))
    lines1.foreachPartition(partition => {
      val redis = pool.getResource
      partition.foreach(x => {
        val document = Map(
          "ts" -> x._1,
        "ip" -> x._2,
        "url" -> x._3,
        "agent" -> x._4,
        "sno" -> x._5
        )
        implicit val formats = DefaultFormats
        val str = Json(DefaultFormats).write(document)
        redis.lpush(s"spark_dump_results", str)
      })
      redis.close()
    })
    sc.stop()
  }
}
