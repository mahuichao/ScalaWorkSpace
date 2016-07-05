import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 2016/6/12.
 */
object TransformTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("myAPP").setMaster("local[2]")
    val sc = new SparkContext(conf)


  }
}
