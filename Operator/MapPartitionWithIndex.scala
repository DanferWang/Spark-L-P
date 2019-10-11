package Operator

import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionWithIndex {
  def main(args: Array[String]): Unit = {
    //Spark配置信息
    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]")
    //创建上下文对象
    val sc = new SparkContext(conf)
    //从内存创建RDD
    val listRDD = sc.makeRDD(1 to 10)
    val indexRDD = listRDD.mapPartitionsWithIndex({
      (num, datas) => {
        datas.map((_, "分区号：" + num))
      }
    })
    indexRDD.collect().foreach(println)

  }

}
