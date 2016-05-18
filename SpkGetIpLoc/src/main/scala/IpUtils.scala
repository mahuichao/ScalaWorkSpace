/**
 * Created by Administrator on 2016/5/18.
 */


import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object IpUtils {
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }


  def binarySearch(lines: Array[(String, String, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]) {
    //    val ip = "118.144.130.10"
    //    val ipNum = ip2Long(ip)
    //    println(ipNum)
    //    //    val lines = readData("D://ip.txt")
    //    val line = Source.fromFile("D://ip.txt").getLines()
    //    val lines = ArrayBuffer[String]()
    //    for (i <- line)
    //      lines += i
    //    val index = binarySearch(lines, ipNum)
    //    print(lines(index))

    val conf = new SparkConf().setAppName("Iplocation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ipRulesRdd = sc.textFile("D://ip.txt").map(line => {
      val fields = line.split("\\|")
      val start_num = fields(2)
      val end_num = fields(3)
      val province = fields(6)
      (start_num, end_num, province)
    })
    // 全部的ip映射规则
    val ipRulesArray = ipRulesRdd.collect()
    // 广播规则
    val ipRules = sc.broadcast(ipRulesArray)
    // 加载要处理的数据
    val ipsRdd = sc.textFile("D://access_log").map(lines => {
      val fields = lines.split("\\|")
      fields(1)
    })
    val result = ipsRdd.map(ip => {
      val ipNum = ip2Long(ip)
      val index = binarySearch(ipRulesArray, ipNum)
      val infro = ipRules.value(index)
      infro
    })
    println(result.collect().toBuffer)
  }

}
