/**
 * Created by Administrator on 2016/6/14.
 */
object Test01 {
  def main(args: Array[String]) {
    val s = ("2016-05-31 (2) 23:52:09","111.206.25.142","xuesheng5121024","172.17.13.2")
//    val line1 = s.filter(x => x._2.contains(".js") || x._2.contains(".css") || x._2.matches("^[0-9]+.*") || x._2.matches("^/.*") || (!x._2.contains("/")))
    val regex = """(.*?)/.*""".r
    val regex(match_host) = s
    println(match_host)
  }

}
