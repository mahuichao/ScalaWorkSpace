import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import sun.misc.{BASE64Decoder, BASE64Encoder}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import com.owlike.genson.defaultGenson._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, FileUtil, Path}

/**
  * Created by mahuichao on 16/10/21.
  * 此版与上版唯一区别就是输入的传入的base64格式json串为Map[String,List[Map[String,Map[String]]]]类型的,
  * 如:{ "condition": [ {"59": { "001": "baidu","tags": "百度狂人"}},{"59":{"001":"a","tags":"a狂人"}}]}
  * 这样做的好处就是可以对同一字段同一规则进行多个不同的设置,从而打上不同的标签。比如上面,如果包含百度,打上百度狂人标签
  * 如果包含a,打上a狂人标签,结果如下:
  * {"stuNum":"1309111261","tags":["a狂人"]}
  * {"stuNum":"130911121","tags":["百度狂人","a狂人"]}
  * {"stuNum":"1309111218","tags":["a狂人"]}
  * {"stuNum":"1309111214","tags":["a狂人"]}
  *
  */
object UnionJob02 {
  val path = "/tmp/result.txt"

  val writer = new PrintWriter(new File(path))
  val hconf = new Configuration()
  val fs = FileSystem.get(hconf)

  // 结果保存路径
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("union")
    val sc = new SparkContext(conf)

    val task_id = args(0) // 接受参数:任务id
    val start_date = args(1) // 接受参数:开始日期
    val end_date = args(2) // 接受参数:结束日期
    val base_path = args(3) // 接受参数:url路径
    val auth_path = args(4) // 接受参数:认证日志路径
    val base64_tags = args(5) // base64格式json
    val result_path = args(6) // 结果保存路径

    /**
      * 这里base_path中url路径与base64我用####来进行分隔
      */

    //    val base64_tags = base_path_origin.split("####")(1)
    val tags_json = getFromBase64(base64_tags)
    /**
      * 此处tags_json格式类似于:{ "condition": [ {"59": { "001": ".*","tags": "百度狂人"}}]}
      *
      */

    val tags_map = parseJson2Map(tags_json)

    val days = getAllDay(start_date, end_date)
    val urls = new ArrayBuffer[String]()
    val auths = new ArrayBuffer[String]()
    for (day <- days) {
      urls += base_path + day + "/*"
      auths += auth_path + day + ".txt"
    }
    val url_paths = urls.toArray.mkString(",")
    val auth_paths = auths.toArray.mkString(",")
    val url_lines = sc.textFile(url_paths) // 所有url日志数据
    val auth_lines = sc.textFile(auth_paths) // 所有认证日志

    // 总共三个参数:ip、time、uid
    val authArrays = auth_lines.map { line =>
      val items = line.split(",")
      val ip = if (items.size >= 3) items(0) else ""
      val time = if (items.size >= 3) items(1) else ""
      val uid = if (items.size >= 3) items(2) else ""
      val key = time.split(" ")(0) + "##" + ip
      (key, (ip, uid, time))
    }.filter {
      case (x, (y, z, n)) => if (x.isEmpty || y.isEmpty || z.isEmpty || n.isEmpty) {
        false
      } else {
        true
      }
    }.collect()
    val broTags = sc.broadcast(tags_map) // 把规则广播出去 tags_map的格式为Map[String,List[Map[String,Map[String,String]]]]
    val broAuthArrays = sc.broadcast(authArrays)
    val format = new SimpleDateFormat("yyyyMMdd")
    val nedFormat = new SimpleDateFormat("yyyy-MM-dd")
    val fullFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
    // 对目录进行检索,查看文件是否存在

    val exists = fs.exists(new org.apache.hadoop.fs.Path("hdfs://iZ2zegttapr07b83jpwolsZ:8020/user/hdfs/results/" + task_id))
    if (exists) {
      // 如果目录已经存在,那么我们就删除目录
      println("指定保存目录已经存在,正在进行删除")
      val ifDel = fs.delete(new org.apache.hadoop.fs.Path("hdfs://iZ2zegttapr07b83jpwolsZ:8020/user/hdfs/results/" + task_id), true)
      if (ifDel) {
        println("删除完成")
      }
    }

    val final_result = url_lines.map { line =>
      val items = line.split(",")
      /*========================开始进行规则匹配Start============================*/
      //      val it = broTags.asInstanceOf[Map[String, Map[String, String]]].keys.iterator

      // 新加入的内容Start
      val tagsList = broTags.value("condition")
      val p_tags = ArrayBuffer[String]()
      for (tagMap <- tagsList) {


        // 新加入的内容End
        val it = tagMap.keys.iterator
        var flag = true
        while (it.hasNext) {
          flag = true
          val key = it.next() // 其实也就是index
          val rules_001 = tagMap(key).getOrElse("001", "")
          val rules_002 = tagMap(key).getOrElse("002", "")
          val tags = tagMap(key).getOrElse("tags", "")
          if (rules_001 != "") {
            val fields = items(key.toInt)
            flag = flag && regFuc(fields, rules_001)
          }
          if (rules_002 != "") {
            val fields = items(key.toInt)
            flag = flag && ifIn(fields, rules_002)
          }
          if (rules_001 == "" && rules_002 == "") {
            // 证明没有标签,本条舍弃
            flag = false
          }
          if (flag == true) {
            val each_tags = tags.split(",")
            for (t <- each_tags) {
              p_tags += t
            }
          }
        }
      }
      if (p_tags.size == 0) {
        // 证明有条件并没有满足
        ("", ("", "", "", ""))
      } else {
        val row_tag = p_tags.mkString(",")
        val url = if (items.size >= 60) items(59) else ""
        val date = if (items.size >= 60) items(17) else ""
        val time = if (items.size >= 60) items(49) else ""
        val host_ip = if (items.size >= 60) items(64) else ""
        val tmpDate = format.parse(date)
        val dateStr = nedFormat.format(tmpDate)
        val fulDate = dateStr + " " + time
        val key = fulDate.split(" ")(0) + "##" + host_ip
        (key, (url, host_ip, fulDate, row_tag))
      }
      /*========================结束规则匹配End============================*/

    }.filter(!_._2._1.isEmpty).map {
      case (xx: String, (y: String, n: String, z: String, t: String)) =>
        var re = ("", Array[String]())
        breakable {
          for ((x1, (y1, n1, z1)) <- broAuthArrays.value) {
            if (xx == x1) {
              val z_time = fullFormat.parse(z)
              val z1_time = fullFormat.parse(z1)
              val stp_z = z_time.getTime()
              val stp_z1 = z1_time.getTime()
              if (stp_z - stp_z1 <= 7200000) {
                // 如果时间差在两个小时以内,确定为ip 人对应无误
                re = (n1, t.split(","))
                break()
              } else {
              }
            } else {
            }
          }
        }
        re
    }.filter(!_._1.isEmpty).groupByKey().map {
      case (x, y) =>
        (x, y.flatMap(m => m).toList.distinct)
    }.map {
      case (x, y) =>
        toJson(Map("stuNum" -> x, "tags" -> y))
    }

    final_result.repartition(1).saveAsTextFile(result_path + task_id)


    //    final_result.saveAsTextFile(result_path + task_id)
    //    val os = fs.create(new Path(result_path + task_id + ".txt"))
    //    val it = final_result.collect().toBuffer.iterator
    //    while (it.hasNext) {
    //      val itLine = it.next()
    //      os.write(itLine + " \n")
    //      os.writeChars(itLine + " \n")
    //      os.writeUTF(itLine + " \n")
    //    }
    //    os.close()

    sc.stop()
  }


  //  用来获取两个日期之间的所有日期

  def getAllDay(startDay: String, endDay: String): Array[String] = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val startDate = format.parse(startDay)
    val endDate = format.parse(endDay)
    var s = true
    var tmpDate = startDate
    val a = new ArrayBuffer[String]()
    a += startDay
    if (!(startDay == endDay)) {
      while (s) {
        val cal = Calendar.getInstance()
        cal.setTime(tmpDate)
        cal.add(Calendar.DAY_OF_MONTH, 1)
        tmpDate = cal.getTime
        val nextDay = format.format(tmpDate)
        a += nextDay
        if (nextDay == endDay) {
          s = false
        }
      }
    }
    return a.toArray
  }

  // 加密
  def getBase64(str: String): String = {
    val b = str.getBytes("utf-8")
    val s = new BASE64Encoder().encode(b)
    return s
  }

  // 解密
  def getFromBase64(str: String): String = {
    val decoder = new BASE64Decoder
    val b = decoder.decodeBuffer(str)
    val result = new String(b, "utf-8")
    return result
  }

  // 正则匹配
  def regFuc(data: String, reg: String): Boolean = {
    val matchReg = reg.r
    val matcher = matchReg.findAllIn(data)
    if (matcher.hasNext) {
      true
    } else {
      false
    }
  }

  // 包含
  def ifIn(data: String, word: String): Boolean = {
    if (data.contains(word)) {
      true
    } else {
      false
    }
  }

  // 把json数据转化为Map类型输出
  def parseJson2Map(str: String): Map[String, List[Map[String, Map[String, String]]]] = {
    val b = fromJson[Map[String, List[Map[String, Map[String, String]]]]](str)
    return b
  }

}
