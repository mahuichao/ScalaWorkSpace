import RedisClient._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

/**
  * Created by mahuichao on 16/9/12.
  */
object DynamicSQLQuery {
  val KEY = "spark_tasks_sql"

  /*
   * redis 中数据格式
  *"{\"files\":[\"hdfs://120.25.177.114:9000/user/root/hdfs/file1.txt\",
  * \"hdfs://120.25.177.114:9000/user/root/hdfs/file2.txt\"],\"fields\":\"name,age,location\"
  * ,\"table_name\":\"human\",\"query_sql\":\"select * from human\",\"dump_redis_key\":\"human_3\"}"
  */

  // origin json structure
  // {
  //    files: ["aaa", "bbb"],
  //    fields: "f1,f2",
  //    table_name: "table1",
  //    query_sql: "select count(1) from table1",
  //    dump_redis_key: "ke1"
  // }
  case class Task(files: Set[String],
                  fields: String,
                  table_name: String,
                  query_sql: String,
                  dump_redis_key: String
                 ) extends Serializable

  // get a job id from args
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DynamicSQLQuery").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("FATAL")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //    val task = get_task_from_redis(args(0))
    val task = get_task_from_redis("3")
    //    println(task.toString)
    val schema =
    StructType(
      task.fields.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    println(task.files.mkString(","))


    val base_data = sc.textFile(task.files.mkString(","))

    val rowRDD = base_data.map(_.split(",", -1)).map(p => Row.fromSeq(p))
    //    val rowRDD = base_data.map(_.split(",", -1)).map(p => Row(p(0), p(1), p(2)))
    val dataSchemaRDD = sqlContext.applySchema(rowRDD, schema)
    dataSchemaRDD.registerTempTable(task.table_name)

    val results = sqlContext.sql(task.query_sql)
    //    println(results.collect()(1))
    val client = pool.getResource
    //    client.select(13)
    results.collect().foreach(x => client.lpush(task.dump_redis_key, x.mkString(",")))
    client.close()
  }

  def get_task_from_redis(id: String): Task = {
    implicit val formats = DefaultFormats
    val client = pool.getResource
    //    client.select(13)
    val condition = client.hget(KEY, id)
    client.close()
    parse(condition).extract[Task]
  }
}
