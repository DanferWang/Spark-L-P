package Advance

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object K_MeansOnWatermelon4 {
  def main(args: Array[String]): Unit = {
    //spark配置
    var sparkConf: SparkConf = new SparkConf().setAppName("K_MeansOnWatermelon4").setMaster("local")
    var sparkContext = new SparkContext(sparkConf)

    //导入数据
    val originData: RDD[String] = sparkContext.textFile("data/kmeans_watermelon4.0.txt")
    //修饰数据
    val toArrayData = originData.map(data=>data.split(",")).map(line=>(line(1),line(2)))
    //向量化
    val vector: RDD[linalg.Vector] = toArrayData.map(row=>Vectors.dense(row._1.toDouble,row._2.toDouble))
    //设置K_Means参数
    val clusters: KMeansModel = new KMeans().setMaxIterations(500).setK(3).run(vector)
    //输出聚类中心
    (0 to 2).foreach(id=>println("cluster"+(id+1)+"'s center is"+clusters.clusterCenters(id)))
    //计算欧几里德距离
    println("distance is "+clusters.computeCost(vector))
    //预测

  }

}
