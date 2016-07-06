import java.io.PrintWriter

import scala.io.Source
import scala.util.matching.Regex

/**
 * 编写Scala程序，从一个带有制表符的文件读取内容，将每个制表符替换成一组空格，使得制表符隔开的n列仍然保持纵向对齐，并将结果写入同一个文件
 * Created by Administrator on 2016/7/6.
 */

object Ex02 {
  def main(args: Array[String]) {
    val file = Source.fromFile("D:/test.txt")
    val writer = new PrintWriter("D:/retest.txt")
    val lineIterator = file.getLines()
    val lineArray = lineIterator.toArray
    val dataPattern = new Regex("(\\t+)", "g1")
    lineArray.foreach { line => {
      val repl = dataPattern.replaceAllIn(line, m => " ")
      writer.write(repl + "\n")
    }
    }
    file.close()
    writer.close()


  }
}
