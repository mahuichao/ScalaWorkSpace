
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.scalactic._
import scala.util.Try

import spark.jobserver._

/**
  * Created by mahuichao on 16/9/6.
  */
object MyTest extends SparkJob {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("helloworld1")
    val sc = new SparkContext(conf)
    val config = ConfigFactory.parseString("")
    val results = runJob(sc, config)
    println("Result is " + results)


  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("input.string"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.string config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Any = {
    val rdd1 = sc.textFile("hdfs://120.25.177.114:9000/user/root/hello.txt")
    val rdd2 = rdd1.flatMap { line =>
      val fields = line.split(" ")
      fields
    }.map((_, 1)).reduceByKey(_ + _)
    rdd2.collect().toBuffer
  }


}
