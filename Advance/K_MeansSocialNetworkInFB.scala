package Advance

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object K_MeansSocialNetworkInFB {
  def main(args: Array[String]): Unit = {
    //Spark配置
    val sparkConf = new SparkConf().setAppName("SocialNetworkInFB").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    //配置聚类中心数量、最大迭代次数
    var K = 5
    var maxIterations = 100

    //读取数据，构建网络
    val originData = sparkContext.textFile("data/facebook/0.edges")
    //邻接矩阵输入参数，值取-1.0，为了后续L=D-S直接使用union方法
    val adjMatrixSEntry: RDD[MatrixEntry] = originData.map(_.split(' ')).map {
      case Array(id1, id2) => {
        MatrixEntry(id1.toLong - 1, id2.toLong - 1, -1.0)
        MatrixEntry(id2.toLong - 1, id1.toLong - 1, -1.0)
      }
    }
    //如果不检查矩阵生成情况，这步也没啥用：读取到CoordinateMatrix，构建邻接矩阵S，按元素储存
    val adjMatrixS: CoordinateMatrix = new CoordinateMatrix(adjMatrixSEntry)
    //检查输出矩阵信息
    println("Number of Users:" + adjMatrixS.numRows() + "  Number of edges:" + originData.count())

    //计算用户度矩阵D
    //带索引的以行存储的行向量
    val rows: RDD[IndexedRow] = adjMatrixS.toIndexedRowMatrix().rows
    //度矩阵输入参数，值为行值的和
    val degreeMatrixDEntry: RDD[MatrixEntry] = rows.map {
      row => MatrixEntry(row.index, row.index, -row.vector.toArray.sum)
    }
    //这步并没有什么用：读取到CoordinateMatrix，构建度矩阵D，按元素储存
    //val degreeMatrixD = new CoordinateMatrix(degreeMatrixDEntry)

    //计算Laplace矩阵，L=D-S
    val laplaceMatrix: CoordinateMatrix = new CoordinateMatrix(degreeMatrixDEntry.union(adjMatrixSEntry))

    //选取聚类中心个数K，计算L前K个特征向量（行）,转置为列向量，组成特征矩阵
    val featuredMatrix: Matrix = laplaceMatrix.toRowMatrix().computePrincipalComponents(K).transpose
    //分为K组，序列化
    val nodes: Seq[Array[Double]] = featuredMatrix.toArray.grouped(K).toSeq
    //向量化
    val vectors: Seq[linalg.Vector] = nodes.map(node=>Vectors.dense(node))
    //必须：重新开辟RDD保存
    val newVectorsRDD = sparkContext.makeRDD(vectors)

    //K_Means设置参数，运行
    val kMeansModel = new KMeans().setK(K).setMaxIterations(maxIterations).run(newVectorsRDD)
    //直观没啥意义：输出聚类特征向量中心
    (0 until K).foreach(id=>println("the center of cluster"+(id+1)+" is"+kMeansModel.clusterCenters(id)))
    //输出欧几里德距离
    println("the minimum cost is"+kMeansModel.computeCost(newVectorsRDD))
    //输出每个用户所在中心
    val result: RDD[(Int, Iterable[Long])] = kMeansModel.predict(newVectorsRDD).zipWithIndex().groupByKey().sortByKey()
    result.collect().foreach{
      res=>println("users in cluster"+(res._1+1)+" :")
        res._2.foreach(user=>print(user+"\t"))
        println()
    }
    //预测

  }
}
