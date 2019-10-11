package SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}

object UDAF_Class {
  def main(args: Array[String]): Unit = {
    //SparkConf
    val conf = new SparkConf().setAppName("SparkSql").setMaster("local")
    //SparkSession
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._

    val frame: DataFrame = session.read.json("data/user.json")

    //强类型自定义聚合函数
    //创建聚合函数对象
    val avgAge = new MyAgeAvgClassFunction

    //将聚合函数转换为查询列
    val avgCol = avgAge.toColumn.name("avgAge")

    val userDS = frame.as[UserData]
    userDS.select(avgCol).show()

    //释放资源
    session.stop()

  }

}

case class UserData(name: String, age: BigInt)

case class AvgBuffer(var sum: BigInt, var count: Int)

//实现方法
class MyAgeAvgClassFunction extends Aggregator[UserData, AvgBuffer, Double] {
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  override def reduce(b: AvgBuffer, a: UserData): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

}
