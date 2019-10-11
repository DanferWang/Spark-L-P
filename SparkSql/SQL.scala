package SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SQL {
  def main(args: Array[String]): Unit = {
    //SparkConf
    val conf = new SparkConf().setAppName("SparkSql").setMaster("local")
    //SparkSession
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val frame: DataFrame = session.read.json("data/user.json")

    //创建临时视图
    frame.createTempView("student")

    //用SQL访问
    session.sql("select name from student").show()

    //释放资源
    session.stop()

  }

}
