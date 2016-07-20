import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2016/7/19.
 */
//取关键字 topN
object Mission02 {
  def main(args: Array[String]) {
    //    val conf = new SparkConf().setAppName("test")
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //    val path = "hdfs://iZ23j3qm3o7Z:8020/user/hdfs/ac_logs/action/**"
    val path = "D://mission.log"
    val file = sc.textFile(path)
    val filteredLines = file.map { line => {
      val fields = line.split(",", -1)
      val host = fields(27)
      val url = if (fields.length > 60) fields(59) else ""
      (host, url)
    }
    }.filter {
      case (x, y) => {
        if (x == "" || x.equals("")) {
          false
        } else {
          true
        }
      }
    }.map { case (x, y) => (x, 1) }.reduceByKey(_ + _).sortBy(_._2).collect().takeRight(100)
    println(filteredLines.toBuffer)
    //    sc.stop()
  }

}
