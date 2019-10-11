package SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Read {
  def main(args: Array[String]): Unit = {
    //SparkConf
    val conf = new SparkConf().setAppName("SparkSql").setMaster("local")
    //SparkSession
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val frame: DataFrame = session.read.json("data/user.json")

    frame.show()

    //释放资源
    session.stop()

  }

}
