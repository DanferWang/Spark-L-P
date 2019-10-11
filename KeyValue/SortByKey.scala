package KeyValue

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object SortByKey {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("SparkTest").setMaster("local")
    var sc = new SparkContext(conf)

    var tupleRDD = sc.makeRDD(Array((3,"a"),(4,"b"),(1,"c"),(2,"d")))
    val sortedRDD = tupleRDD.sortByKey(true)
    sortedRDD.collect().foreach(println)
  }

}