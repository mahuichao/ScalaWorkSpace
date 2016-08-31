import java.text.SimpleDateFormat

import scala.io.Source

/**
  * Created by mahuichao on 16/8/29.
  */
object Test02 {
  def main(args: Array[String]): Unit = {
    //    val file = Source.fromFile("/Users/mahuichao/test.txt")
    val file = Source.fromFile("/Users/mahuichao/auth.txt")
    val lines = file.getLines()
    while (lines.hasNext) {
      //          //      println(lines.next())
      val line = lines.next()
      //          val items = line.split(",")
      //          val times = items(49)
      //          val date = items(17)
      //          val curDate = date + times
      //          val dateFormat = new SimpleDateFormat("yyyyMMddHH:mm:ss")
      ////          val dates = dateFormat.parse(curDate)
      ////          val tmp = new java.sql.Timestamp(dates.getTime)
      ////          val utm = tmp.getTime
      //          println(utm)
      //        }

      //    val lines = file.getLines()
      //    while (lines.hasNext) {
      //      val line = lines.next()
      //      val r_ip ="""USERIP=(.*?),""".r
      //      val r_login_time ="""LOGINTIME=(.*?),""".r
      //      val r_logout_time ="""LOGOUTTIME=(.*?),""".r
      //      val r_account_id ="""ACCOUNTID=(.*?),""".r
      //      val ip_match = r_ip.findAllIn(line)
      //      val login_time_match = r_login_time.findAllIn(line)
      //      val logout_time_match = r_logout_time.findAllIn(line)
      //      val account_id_match = r_account_id.findAllIn(line)
      //      var ip = ""
      //      var login_time = 0l
      //      var logout_time = 0l
      //      var account_id = ""
      //      while (ip_match.hasNext) {
      //        ip = ip_match.group(1)
      //        ip_match.next()
      //      }
      //      while (login_time_match.hasNext) {
      //        login_time = login_time_match.group(1).toLong
      //        login_time_match.next()
      //      }
      //      while (logout_time_match.hasNext) {
      //        logout_time = logout_time_match.group(1).toLong
      //        logout_time_match.next()
      //      }
      //      while (account_id_match.hasNext) {
      //        account_id = account_id_match.group(1)
      //        account_id_match.next()
      //      }
      //      println("ip: " + ip)
      //      println("login: " + login_time)
      //      println("logout: " + logout_time)
      //      println("account: " + account_id)


      val dateFormat = new SimpleDateFormat("yyyyMMdd")
      val items = line.split(",")
      val r_ip ="""USERIP=(.*?),""".r
      val r_login_time ="""LOGINTIME=(.*?),""".r
      val r_logout_time ="""LOGOUTTIME=(.*?),""".r
      val r_account_id ="""ACCOUNTID=(.*?),""".r
      val ip_match = r_ip.findAllIn(line)
      val login_time_match = r_login_time.findAllIn(line)
      val logout_time_match = r_logout_time.findAllIn(line)
      val account_id_match = r_account_id.findAllIn(line)
      var ip = ""
      var login_time = 0
      var logout_time = 0
      var account_id = ""
      while (ip_match.hasNext) {
        ip = ip_match.group(1)
        ip_match.next()
      }
      while (login_time_match.hasNext) {
        // dateFormat.parse(curdatestr).toString.toInt
        val curdatestr = dateFormat.format(login_time_match.group(1).toLong)
        login_time = curdatestr.toInt
        login_time_match.next()
      }
      while (logout_time_match.hasNext) {
        // dateFormat.parse(curdatestr).toString.toInt
        val curdatestr = dateFormat.format(logout_time_match.group(1).toLong)
        logout_time = curdatestr.toInt
        logout_time_match.next()
      }
      while (account_id_match.hasNext) {
        account_id = account_id_match.group(1)
        account_id_match.next()
      }
      println(ip)
      println(login_time)
      println(logout_time)
      println(account_id)
    }
  }
}