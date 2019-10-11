package KeyValue

import org.apache.spark.{SparkConf, SparkContext}

object Join {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local").setAppName("SparkTest")
    var sc = new SparkContext(conf)

    var rdd1 = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c"),(4,"d")))
    var rdd2 = sc.makeRDD(Array((1,"A"),(2,"B"),(3,"C"),(4,"D")))

    val joinRDD = rdd1.join(rdd2)
    joinRDD.collect().foreach(println)
  }

}
