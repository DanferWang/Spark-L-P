package Advance

import org.apache.spark.graphx.{Edge, Graph, GraphOps, VertexId}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LogisticsOnSignedSocialNetwork {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogisticsOnSignedSocialNetwork").setMaster("local")
    val context = new SparkContext(conf)

    //载入数据，预处理数据格式
    val data: RDD[String] = context.textFile("data/soc-sign-epinions.txt")
    val dataSet: RDD[(Long, Long, Int)] = data.map(s => s.split("\t") match {
      case Array(id1, id2, sign) => (id1.toLong, id2.toLong, sign.toInt)
    })

    //定义结点、边，构建图
    val nodes: RDD[(VertexId, String)] = context.parallelize(0L until 131828L).map(id=>(id,id.toString))
    val edges: RDD[Edge[String]] = dataSet.filter(_._3 == 1).map {
      case (id1, id2, sign) => Edge(id1, id2, id1 + "->" + id2)
    }
    val network = Graph(nodes,edges)
    val ops = network.ops

    //检查结点、边的数量
    println("Number of nodes = "+ops.numVertices)
    println("Number of edges = "+ops.numEdges)

    //以结点的度作为分类特征
    val degrees: RDD[(VertexId, Int)] = ops.degrees.map{case (id,degree)=>(id.toLong,degree)}
    val inDegrees: RDD[(VertexId, Int)] = ops.inDegrees.map{case (id,degree)=>(id.toLong,degree)}
    val outDegrees: RDD[(VertexId, Int)] = ops.outDegrees.map{case (id,degree)=>(id.toLong,degree)}

    //整合特征信息，形成数据条
    val dataLine: RDD[(Int, Int, Int, Int, Int, Int, Int)] = dataSet.map {
      case (id1, id2, sign) => (id1, (id2, sign))
    }.join(degrees).join(inDegrees).join(outDegrees).map {
      case (id1, ((((id2, sign), degree1), inDegree1), outDegree1))
      => (id2, (id1, sign, degree1, inDegree1, outDegree1))
    }.join(degrees).join(inDegrees).join(outDegrees).map {
      case (id2, ((((id1, sign, degree1, inDegree1, outDegree1), degree2), inDegree2), outDegree2))
      => (sign, degree1, inDegree1, outDegree1, degree2, inDegree2, outDegree2)
    }

    //标记正例、反例
    val labeledData: RDD[LabeledPoint] = dataLine.map {
      case (s, d1, d2, d3, d4, d5, d6) =>
        if (s == 1)
          LabeledPoint(1.0, Vectors.dense(d1, d2, d3, d4, d5, d6))
        else
          LabeledPoint(0.0, Vectors.dense(d1, d2, d3, d4, d5, d6))
    }
    val positiveData = labeledData.filter(_.label == 1.0)
    val negativeData = labeledData.filter(_.label == 0.0)

    //划分训练集、验证集
    val positiveSplit = positiveData.randomSplit(Array(0.6,0.4),4L)
    val negativeSplit = negativeData.randomSplit(Array(0.6,0.4),4L)
    val training = positiveSplit(0).union(negativeSplit(0))
    val validating = positiveSplit(1).union(negativeSplit(1))

    //训练Logistics模型
    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training)

    //检查该模型在验证集上的准确率和召回率
    val predictionAndLabel = validating.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }
    val metrics = new MulticlassMetrics(predictionAndLabel)
    println("the precision is "+metrics.precision(1.0))
    println("the recall is "+metrics.recall(1.0))

    //输出每个特征的权值
    val weights = model.weights.toArray.zipWithIndex
    weights.foreach { case (w, i) =>
        println("Feature "+ i + " is " + w)
    }

  }

}
