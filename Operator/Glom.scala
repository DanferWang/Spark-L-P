package Operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Glom {
  def main(args: Array[String]): Unit = {
    //Spark配置信息
    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]")
    //创建上下文对象
    val sc = new SparkContext(conf)
    //从内存创建RDD
    val listRDD: RDD[Int] = sc.makeRDD(1 to 16 , 4)
    //将每一个分区放入独立的数组
    val glomRDD = listRDD.glom()
    glomRDD.collect().foreach(array=>println(array.mkString(",")))

  }
}
