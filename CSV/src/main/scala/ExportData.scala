import java.io.FileWriter

import RedisClient._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * Created by mahuichao on 16/8/29.
  */

object ExportData {
  // 保存文件
  var file: FileWriter = null
  // 保存路径含有
  val PATH = "/Users/mahuichao"


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("importData").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("FATAL")
    //    val log_path = "hdfs://120.25.177.114:50090/log/test.csv";
    val log_path = "/Users/mahuichao/Downloads/test.csv"
    val tableName = "action"
    startWork(log_path, sc, tableName)

  }

  // 开始任务 参数说明:path为保存路径,sc为spark上下文环境,tableName为表名字
  def startWork(log_path: String, sc: SparkContext, tableName: String): Unit = {
    val logs = sc.textFile(log_path) // 日志RDD
    file = new FileWriter(PATH + "/" + tableName, true) // 初始化导出文件
    var cols = List[String]()
    val needCols = getNeed(tableName) // 所需字段
    val conList = getCon(tableName) // 需过滤的条件
    for (s <- conList) {
      val key = s.keys.toIterator.next()
      cols = key :: cols
    }
    val post = cols.distinct // 第几列有要求


    val result = logs.map { line => {
      val items = line.split(",")
      var std = true
      for (p <- post) {
        val field = items(p.toInt) // 待检验的字段
        for (s <- conList) {
          // 001为正则匹配
          std = std && judgeCon(s, p, field, "001")
          // 002为包含
          std = std && judgeCon(s, p, field, "002")
          // 003 为不包含
          std = std && judgeCon(s, p, field, "003")
        }
      }
      if (std == true) {
        // 证明符合测试
        //        val pros = getNeed(tableName) // 所要导出的字段
        var exData = ""
        for (i <- needCols) {
          println("i-------" + i)
          exData = exData.concat("," + items(i.toInt)) // 导出所需字段
        }
        exportData(exData) // 导出字段
      }
      // 一行执行完毕
    }
    }
    result.collect()
    // 所有行执行完毕,关闭文件
    stopFile()

  }


  // 导出数据
  def exportData(data: String): Unit = {
    file.write(data + "\n")
  }

  // 关闭文件
  def stopFile(): Unit = {
    if (file != null) {
      file.close()
    }

  }

  // 判断是那种要求
  def judgeCon(s: Map[String, Map[String, String]], p: String, field: String, filter_type: String): Boolean = {
    var result = true
    // 正则
    if (filter_type == "001") {
      var con_detail = ""
      try {
        con_detail = s(p)(filter_type)
      } catch {
        case e: java.util.NoSuchElementException => con_detail = ""
      } finally {
        if (con_detail.isEmpty) {
          // 证明没有正则要求 默认不符合要求
        } else {
          // 证明有正则要求
          result = regFuc(field, con_detail)
        }

      }

    }
    // 包含
    if (filter_type == "002") {
      var con_detail = ""
      try {
        con_detail = s(p)(filter_type)
        println("con_detail:" + con_detail)
      } catch {
        case e: java.util.NoSuchElementException => con_detail = ""
      } finally {
        if (con_detail.isEmpty) {
          // 证明没有包含要求
        } else {
          // 证明有包含要求
          result = ifIn(field, con_detail)
        }

      }
    }
    // 不包含
    if (filter_type == "003") {
      var con_detail = ""
      try {
        con_detail = s(p)(filter_type)
      } catch {
        case e: java.util.NoSuchElementException => con_detail = ""
      } finally {
        if (con_detail.isEmpty) {
          // 证明没有不包含要求
        } else {
          // 证明有不包含要求
          result = ifNotIn(field, con_detail)
        }

      }
    }

    result
  }

  /**
    * 获取指定字段所要满足的条件 例如 "[{"1":{"001":".*"}},{"18":{"002":"2"}},{"18":{"003":"线路"}}]"
    * 这要求第一个字段要满足001也就是正则,正则匹配的内容是.* 同时18列还要满足002也就是包含 包含的内容为2.。。。
    * 等上述条件全部满足,才会进行导出指定字段
    */

  def getCon(tableName: String): List[Map[String, Map[String, String]]] = {
    val client = pool.getResource
    val condition = client.hget(tableName, "condition")
    client.close()
    val con = JSON.parseFull(condition)
    con match {
      case Some(list: List[Map[String, Map[String, String]]]) => list
    }
  }

  /**
    * 获取如果满足条件所需导出字段   "[\"18\",\"19\",\"20\"]"
    * 以上说明想要获取第18、19、20三列
    */

  def getNeed(tableName: String): List[String] = {
    val client = pool.getResource
    val need = client.hget(tableName, "need")
    client.close()
    val nd = JSON.parseFull(need)
    nd match {
      case Some(list: List[String]) => list
    }
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

  // 不包含
  def ifNotIn(data: String, word: String): Boolean = {
    if (data.contains(word)) {
      false
    } else {
      true
    }

  }


}
