import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

/**
 * Created by Administrator on 2016/6/12.
 */
object OfficialTest {
  def main(args: Array[String]) {
    // 更新使用，
    val updateFunction = (itr: Iterator[(String, Seq[Int], Option[Int])]) => {
      itr.flatMap { case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(m => (x, m)) }
    }
    val conf = new SparkConf().setAppName("UpdateStat").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("FATAL")
    sc.setCheckpointDir("D://check1")
    val ssc = new StreamingContext(sc, Seconds(5))

    val words = ssc.socketTextStream("sun", 9999)
    // [(a,1),(b,1)....]
    val wordMap = words.flatMap(_.split(" ")).map((_, 1))
    val result = wordMap.updateStateByKey[Int](updateFunction,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }


}
