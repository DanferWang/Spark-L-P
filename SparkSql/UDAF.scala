package SparkSql
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object UDAF {
  def main(args: Array[String]): Unit = {
    //SparkConf
    val conf = new SparkConf().setAppName("SparkSql").setMaster("local")
    //SparkSession
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._

    val frame: DataFrame = session.read.json("data/user.json")

    //自定义聚合函数
    //创建聚合函数对象
    val avgAge = new MyAgeAvgFunction
    //注册函数
    session.udf.register("avgAge", avgAge)

    //创建临时视图
    frame.createTempView("student")

    //用SQL访问
    session.sql("select avgAge(age) from student").show()

    //释放资源
    session.stop()

  }

}

//继承UserDefinedAggregateFunction
//实现方法
class MyAgeAvgFunction extends UserDefinedAggregateFunction {
  //输入函数的数据结构
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  //计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  //函数返回数据类型
  override def dataType: DataType = DoubleType

  //函数是否稳定
  override def deterministic: Boolean = true

  //函数开始计算之前，缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //根据SQL查询结果更新缓冲区
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //归并（合并）不同分区间的缓冲区数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算缓冲区数据
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
