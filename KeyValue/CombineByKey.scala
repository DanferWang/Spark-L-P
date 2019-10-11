package KeyValue

import org.apache.spark.{SparkConf, SparkContext}

object CombineByKey {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("SparkTest").setMaster("local")
    var sc = new SparkContext(conf)

    var listRDD = sc.makeRDD(List(("a", 1), ("b", 4), ("b", 3), ("a", 2), ("c", 5), ("c", 3)), 3)
    var combineRDD = listRDD.combineByKey((_,1),(acc:(Int,Int),v)=>(acc._1+v,acc._2+1),(acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))
    val avgRDD = combineRDD.map{case (string,tuple)=>(string,tuple._1/tuple._2.toDouble)}
    avgRDD.collect().foreach(println)
  }

}

