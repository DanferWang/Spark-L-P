package Operator

import org.apache.spark.{SparkConf, SparkContext}

object MultiValue {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("SparkTest").setMaster("local")
    var sc = new SparkContext(conf)

    var listRDD1 = sc.makeRDD(1 to 8)
    var listRDD2 = sc.makeRDD(5 to 12)

    //简单合并
    val unionRDD = listRDD1.union(listRDD2)
    unionRDD.collect().foreach(println)
    //合并后去重求并集
    val unionDistinctRDD = listRDD1.union(listRDD2)
    unionRDD.collect().distinct.foreach(println)

    //求差集
    val subtractRDD = listRDD1.subtract(listRDD2)
    subtractRDD.collect().foreach(println)

    //求交集
    val intersectionRDD = listRDD1.intersection(listRDD2)
    intersectionRDD.collect().foreach(println)

    //笛卡尔积
    val cartesianRDD = listRDD1.cartesian(listRDD2)
    println("元组数量："+cartesianRDD.collect().size)
    cartesianRDD.collect().foreach(println)

    //“拉链”合成k-v对
    val zipRDD = listRDD1.zip(listRDD2)
    zipRDD.collect().foreach(println)

  }

}
