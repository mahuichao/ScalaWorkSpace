import org.apache.spark.{SparkContext, HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Administrator on 2016/5/25.
 */
object KafkaWordCount {
  val func = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    iter.flatMap {
      case (x, y, z) => Some(
        y.sum + z.getOrElse(0)
      ).map(i => (x, i))
    }
  }

  def main(args: Array[String]) {
    val Array(zkQuorum, group, topics, numThreads) = args
    val conf = new SparkConf().setAppName("kafkaWordCount").setMaster("local[2]")
    val s = new SparkContext(conf)
    s.setLogLevel("FATAL")
    val sc = new StreamingContext(s, Seconds(5))

    sc.checkpoint("D://ck1")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val data = KafkaUtils.createStream(sc, zkQuorum, group, topicMap)
    val words = data.map(_._2).flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).updateStateByKey(func, new HashPartitioner(sc.sparkContext.defaultParallelism), true)
    wordCounts.print()
    sc.start()
    sc.awaitTermination()
  }

}
