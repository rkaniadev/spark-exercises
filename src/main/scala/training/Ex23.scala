package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, max}

object Ex23 extends App {

  val spark = SparkSession
    .builder()
    .appName("ex9")
    .master("local[*]")
    .getOrCreate()

  // ex. 23
  val input = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load("/tmp/input/ex23.csv")

  input.show

  val w1 = Window
    .partitionBy("department")
    .orderBy(desc("salary"))

  val diff = max(input("salary")).over(w1) - input("salary")

  val result = input.select(
    col("id"),
    col("name"),
    col("department"),
    col("salary"),
    diff.alias("diff")
  )

  result.show()





}
