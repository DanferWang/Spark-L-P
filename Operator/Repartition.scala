package Operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Repartition {
  def main(args: Array[String]): Unit = {
    //Spark配置信息
    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]")
    //创建上下文对象
    val sc = new SparkContext(conf)
    //从内存创建RDD
    val listRDD: RDD[Int]= sc.makeRDD(1 to 16,4)
    println("原始分区数：" + listRDD.partitions.size)
    //重分区
    val repartitionRDD = listRDD.repartition(2)
    println("缩减分区数：" + repartitionRDD.partitions.size)
  }

}
