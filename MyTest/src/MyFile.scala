/**
 * Created by Administrator on 2016/5/15.
 */

import java.io.File

import scala.io.Source
import MyPref._

class MyFile(val f: File) {
  def read() = Source.fromFile(f).mkString
}

object MyFile {
  def main(args: Array[String]) {
    val f = new File("D://test.txt")
    f.read
  }
}

