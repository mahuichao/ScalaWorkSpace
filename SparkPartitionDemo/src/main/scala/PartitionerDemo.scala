import java.net.URL

import org.apache.spark.{Partitioner, Partition, SparkContext, SparkConf}
import scala.collection.mutable

/**
 * Created by Administrator on 2016/5/18.
 */
object PartitionerDemo {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PartitionerDmeo").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("D://itcast.log")
    val rdd1 = lines.map(line => {
      val f = line.split("\t")
      (f(1), 1)
    })
    val rdd2 = rdd1.reduceByKey(_ + _)
    val rdd3 = rdd2.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      (host, (url, t._2))
    })
    val rdd4 = rdd3.map(_._1).distinct().collect()
    val hostPartition = new PartitionerDemo(rdd4)



    //    rdd3.partitionBy(hostPartition).saveAsTextFile("D://out1")
    val rdd5 = rdd3.partitionBy(hostPartition).mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(2).iterator
    })
    rdd5.saveAsTextFile("D://out2")
    println(rdd3.collect().toBuffer)
    sc.stop()
  }


  class PartitionerDemo(ins: Array[String]) extends Partitioner {
    val parMap = new mutable.HashMap[String, Int]()
    var count = 0
    for (i <- ins) {
      parMap += (i -> count)
      count += 1
    }

    override def numPartitions: Int = 5

    override def getPartition(key: Any): Int = {
      parMap.getOrElse(key.toString, 0)
    }
  }

}
