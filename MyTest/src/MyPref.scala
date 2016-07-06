import java.io.File

/**
 * Created by Administrator on 2016/5/15.
 */
object MyPref {
  implicit def fileAddRead(f: File) = new MyFile(f)

  implicit def girl2Ordered(g: Girl) = new Ordered[Girl] {
    override def compare(that: Girl): Int = {
      g.faceValue - that.faceValue
    }
  }
}
