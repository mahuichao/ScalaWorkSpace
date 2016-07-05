import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionModel, LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Administrator on 2016/7/5.
 */
object Day05_Linear {
  def main(args: Array[String]): Unit = {
    // 构建spark对象
    val conf = new SparkConf().setAppName("linear").setMaster("local[2]")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)
    //  读取样本集
    val data_path1 = "file:///D:/num1.txt"
    val data = sc.textFile(data_path1)
    //    println(data.collect().toBuffer)
    val examples = data.map { line =>
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }.cache()

    val numExamples = examples.count()

    // 新建线性回归模型，并设置训练参数
    val numIterations = 100
    val stepSize = 1
    val miniBatchFraction = 1.0

    val model = LinearRegressionWithSGD.train(examples, numIterations, stepSize, miniBatchFraction)
    model.weights
    model.intercept

    // 对样本进行测试
    val prediction = model.predict(examples.map(_.features))
//    println("---------------" + prediction.collect().toBuffer)
    val predictionAndLabel = prediction.zip(examples.map(_.label))
    val print_predict = predictionAndLabel.take(50)
    println("prediction" + "\t" + "label")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }
    // 计算测试误差
    val loss = predictionAndLabel.map {
      // 后边那个是L的小写，不是1
      case (p, l) =>
        val err = p - l
        err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / numExamples)
    println(s"Test RMSE=$rmse.")

    // 模型保存
    val ModelPath = "file:///D:/save.txt"
    model.save(sc, ModelPath)
    val sameModel = LinearRegressionModel.load(sc, ModelPath)
  }

}
