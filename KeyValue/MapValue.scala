package KeyValue

import org.apache.spark.{SparkConf, SparkContext}

object MapValue {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("SparkTest").setMaster("local")
    var sc = new SparkContext(conf)

    var tupleRDD = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c"),(4,"d")))
    val mapValuesRDD = tupleRDD.mapValues(_+"**")
    mapValuesRDD.collect().foreach(println)
  }

}
