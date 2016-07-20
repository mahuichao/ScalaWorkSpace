import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2016/7/19.
 */
// 取host访问topN
object Mission01 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    //    val conf = new SparkConf().setAppName("test")
    val sc = new SparkContext(conf)
    //    val path = "hdfs://iZ23j3qm3o7Z:8020/user/hdfs/ac_logs/action/**"
    val path = "D://mission.log"
    val file = sc.textFile(path)
    val keyRegex = """wd=(.*?)&""".r
    val filteredLines = file.map { line => {
      val fields = line.split(",", -1)
      val host = fields(27)
      val url = if (fields.length > 60) fields(59) else ""
      (host, url)
    }
    }.filter {
      case (x, y) => {
        if (y.contains("www.baidu.com/s?wd=")) {
          true
        } else {
          false
        }
      }
    }.map {
      case (host, url) => {
        val key = keyRegex.findAllIn(url)
        var keyword = ""
        if (key.hasNext) {
          keyword = key.group(1)
        } else {
        }
        (keyword, 1)
      }
    }.filter {
      case (x, y) => {
        if (x == "" || x.equals("")) {
          false
        } else {
          true
        }
      }
    }.reduceByKey(_ + _).sortBy(_._2).collect().takeRight(1)
    println(filteredLines.toBuffer)
    //    sc.stop()

  }
}

