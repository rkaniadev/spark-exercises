package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, dense_rank, desc}

object Ex22 extends App {

  val spark = SparkSession
    .builder()
    .appName("ex9")
    .master("local[*]")
    .getOrCreate()

  val input = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load("/tmp/input/ex22.csv")

  input.show

  // Windowing, something more than operations on group.
  // Using it we can perform a lot of more.

  val window =
    Window.partitionBy("genre").orderBy(desc("quantity"))

  val withRank = input.withColumn("rank", dense_rank() over(window)).orderBy("rank")
  withRank
    .select("id", "title", "genre", "quantity", "rank")
    .where(col("rank") === 1 or col("rank") === 2)
    .orderBy(asc("genre"), desc("quantity"))
    .show

}
