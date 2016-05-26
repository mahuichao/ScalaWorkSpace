import kafka.serializer.StringDecoder
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * mhc
 * Created by Administrator on 2016/5/25.
 */
object KafkaDirect {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("directKafka").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("FATAL")
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("D://ch3")
    val Array(brokers, topics) = args
    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list"->brokers)
    val lines = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicSet)
    lines.foreachRDD { rdd =>
      val result = rdd.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect()
      println(result.toBuffer)
    }

    ssc.start()
    ssc.awaitTermination()
  }


}
