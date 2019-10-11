package KeyValue

import org.apache.spark.{SparkConf, SparkContext}

object FoldByKey {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("SparkTest").setMaster("local")
    var sc = new SparkContext(conf)

    var listRDD = sc.makeRDD(List(("a", 1), ("b", 4), ("b", 3), ("a", 2), ("c", 5), ("c", 3)), 3)
    var foldRDD = listRDD.foldByKey(0)(_+_)
    foldRDD.collect().foreach(println)
  }

}

