package KeyValue

import org.apache.spark.{SparkConf, SparkContext}

object GroupByKey {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("SparkTest").setMaster("local")
    var sc = new SparkContext(conf)

    var listRDD = sc.textFile("data/nasa.txt")
    //转化为k-v对
    val wordTupleRDD = listRDD.flatMap(_.split(" ")).map(word=>(word,1))
    //分组合并
    val wordCountRDD = wordTupleRDD.groupByKey().map(tuple=>(tuple._1,tuple._2.sum))
    wordCountRDD.sortBy(-_._2).take(10).foreach(println)

  }

}
