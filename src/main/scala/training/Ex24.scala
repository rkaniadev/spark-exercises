package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, sum}

object Ex24 extends App {
  val spark = SparkSession
    .builder()
    .appName("ex9")
    .master("local[*]")
    .getOrCreate()

  // ex. 24
  val input24 = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load("/tmp/input/ex24.csv")

  val w1 = Window
    .partitionBy("department")
    .orderBy("items_sold")

  input24.show()

  input24.select(
    col("time"),
    col("department"),
    col("items_sold"),
    sum("items_sold").over(w1).as("running_total")
  ).show()
}
