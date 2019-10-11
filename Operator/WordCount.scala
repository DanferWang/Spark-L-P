package Operator

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val file = sc.textFile("data\\nasa.txt")
    val words = file.flatMap(_.split(" "))
    val wordTuple = words.map((_,1))
    val res = wordTuple.reduceByKey(_+_).sortBy(_._2,false).take(10)
    res.foreach(println)
  }
}
