package KeyValue

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PartitionBy {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("SparkTest").setMaster("local")
    var sc = new SparkContext(conf)

    var tupleRDD = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c"),(4,"d")),4)
    //new一个分区器，对其重新分区
    //HashPartitioner实际上是一个对key取模的分区原则
    val tuplePartitionByPartitionerRDD = tupleRDD.partitionBy(new HashPartitioner(2))
    tuplePartitionByPartitionerRDD.glom().collect.foreach(array=>println(array.mkString(",")))
  }

}