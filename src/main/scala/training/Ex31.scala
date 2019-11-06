package training

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Ex31 extends App {
  // Windowing
  // operate on a group of rows
  // while still returning a single value for every input row

  val spark = SparkSession
    .builder()
    .appName("ex9")
    .master("local[*]")
    .getOrCreate()

  val visits = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv("/tmp/input/ex31.csv")

  visits.show()

  import spark.sqlContext.implicits._

  //TODO find better solution
  val byIdOrderedByTime = Window.partitionBy("ID").orderBy("time")

  val reduceStep1 = reduce(visits)
  reduceStep1.show()
  val reduceStep2 = reduce(reduceStep1)
  reduceStep2.show()
  val result = countElements(reduceStep2)
  result.show()

  def reduce(df: DataFrame): DataFrame = {
    df.withColumn("diff", (lead($"time", 1, null) over byIdOrderedByTime) - $"time")
      .withColumn("diff", when($"diff".isNull, 1).otherwise($"diff"))
      .select("id", "time", "diff")
      .where($"diff" === 1)
  }

  def countElements(df: DataFrame): DataFrame = {
    df.groupBy("id").agg( collect_list("diff").alias("count") )
  }

  // ranking functions
  // Row Frame
  // Range Frame
}