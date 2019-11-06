package Advance

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

object Logistic {
  def main(args: Array[String]): Unit = {
    //配置spark
    val sparkConf = new SparkConf().setAppName("Logistic").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    //载入数据
    //LabeledPoint(label,vector)
    val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sparkContext, "data/sample_libsvm_data.txt")

    //划分训练集、测试集
    val split = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = split(0)
    val test = split(1)

    //训练Logistic模型
    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training)

    //预测测试集
    val predictAndLabel = test.map { case LabeledPoint(label, features) =>
      val predict = model.predict(features)
      (predict, label)
    }
    //输出（预测，标签）
    predictAndLabel.foreach(println)

    //计算准确率、召回率
    val metrics = new MulticlassMetrics(predictAndLabel)
    println("Precision: " + metrics.precision(1.0))
    println("Recall: " + metrics.recall(1.0))

  }

}
