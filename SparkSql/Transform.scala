package SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Transform {
  def main(args: Array[String]): Unit = {
    //SparkConf
    val conf = new SparkConf().setAppName("SparkSql").setMaster("local")
    //SparkSession
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //转换之前需要引入隐式转换
    import session.implicits._

    //创建RDD
    val userRDD: RDD[(Int, String, Int)] = session.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangermazi", 40)))

    //转换为DataFrame
    val userDF: DataFrame = userRDD.toDF("id", "name", "age")

    //转换为DataSet
    val userDS: Dataset[User] = userDF.as[User]

    //转换为DataFrame
    val userDF1: DataFrame = userDS.toDF()

    //转换为RDD
    val userRDD1: RDD[Row] = userDF1.rdd

    userRDD1.foreach(row => {
      //获取数据时，可以通过索引访问数据
      println(row.getString(1))
    })

    session.close()
  }
}

case class User(id: Int, name: String, age: Int)