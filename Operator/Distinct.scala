package Operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Distinct {
  def main(args: Array[String]): Unit = {
    //Spark配置信息
    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]")
    //创建上下文对象
    val sc = new SparkContext(conf)
    //从内存创建RDD
    val listRDD: RDD[Int]= sc.makeRDD(Array(1,2,1,5,2,5,6,9,1))
    //去重
    val distinctRDD = listRDD.distinct()
    distinctRDD.collect().foreach(println)

  }

}
