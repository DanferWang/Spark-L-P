package KeyValue

import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKey {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("SparkTest").setMaster("local")
    var sc = new SparkContext(conf)

    var listRDD = sc.makeRDD(List(("a", 1), ("b", 4), ("b", 3), ("a", 2), ("c", 5), ("c", 3)), 3)
    //查看分区分配
    listRDD.glom().collect().foreach(list => println(list.mkString(",")))
    //分区内按键操作后，再在分区间操作：
    //对每一分区内按键筛选最大值，再在分区间叠加
    val aggregateRDD = listRDD.aggregateByKey(0)(math.max(_, _), _ + _)
    aggregateRDD.collect().foreach(println)
  }

}

