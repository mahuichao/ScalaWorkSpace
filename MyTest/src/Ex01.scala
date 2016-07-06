import java.io.PrintWriter

import scala.io.Source

/**
 * 编写一小段Scala代码，将某个文件中的行倒转顺序，将最后一行作为第一行,依此类推
 * Created by Administrator on 2016/7/6.
 */
object Ex01 {
  def main(args: Array[String]) {
    // 要读取的文件
    val file = Source.fromFile("D:/test1.txt")
    // 读取完毕重新写入的文件
    val writer = new PrintWriter("D:/reTest.txt")
    val lineIterator = file.getLines()
    val lineArray = lineIterator.toArray
    val reLineArray = lineArray.reverse
    reLineArray.foreach {
      line => writer.write(line + "\n")
    }
    file.close()
    writer.close()
  }
}
