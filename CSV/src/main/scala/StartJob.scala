import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mahuichao on 16/8/29.
  */
object StartJob {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("urlfilter").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("FATAL")

    // iir.blizzard.com
    // waimaiapi.meituan.com
    //asdk.xindong.com
    // val logFiles = sc.textFile("hdfs://wtuedu001:8022/user/hdfs/ac_logs/action/*/*")
    val logFiles = sc.textFile("hdfs://wtuedu001:8022/user/hdfs/ac_logs/action/2016080[6-9]/00")
    val result = logFiles.map { x =>
      if (x.contains("asdk.xindong.com")) {
        (x, 1)
      }else if (x.contains("asdk.xindong.com")) {
        (x, 0)
      } else {
        ("", 0)
      }
    }.filter { case (x, y) => {
      if (x != "") {
        true
      } else {
        false
      }
    }
    }.collect()

    println(result.toBuffer)


  }

}
