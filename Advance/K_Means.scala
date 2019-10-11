package Advance

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object K_Means {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local").setAppName("K_Means")
    var sc = new SparkContext(conf)

    //读取数据
    val data: RDD[String] = sc.textFile("data/kmeans_data.txt")
    //将数据转换为向量
    val vector = data.map(s=> Vectors.dense(s.split(' ').map(_.toDouble)))
    //设置Kmean参数
    val clusters: KMeansModel = new KMeans().setK(2).setMaxIterations(10).run(vector)

    (0 to 1).foreach(id=>println("center of cluster"+(id+1)+clusters.clusterCenters(id)))

    //预测
    val point = Vectors.dense(Array(0.5,0.9,0.8))
    val predictResult = clusters.predict(point)+1
    println("Point"+point+"属于cluster"+predictResult)

  }

}
