import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable._

/**
 * Created by Administrator on 2016/6/12.
 */
object MyTest {
  def main(args: Array[String]) {
    val count = new HashMap[String, Integer]
    val conf = new SparkConf().setAppName("My Test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //    sc.setLogLevel("FATAL")
    val ssc = new StreamingContext(sc, Seconds(5))
    val line = ssc.socketTextStream("sun", 9999)
    // [a,b.c,d,e,....]
    val word = line.flatMap(x => {
      x.split(" ")
    }).map(x => {
      if (count.contains(x)) {
        //  如果包含的话
        count(x) += 1
      } else {
        // 不包含，第一次见
        count(x) = 1

      }
      x
    })
    word.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
