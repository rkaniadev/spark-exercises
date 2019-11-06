package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object Ex13_14_15_16 extends App {

  val spark = SparkSession
    .builder()
    .appName("ex13")
    .master("local[*]")
    .getOrCreate()

  val input = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load("/tmp/input/ex13.csv")
    .withColumn("population",
      regexp_replace(col("population"), " ", "").cast(IntegerType))

  // 13
  input.cache()
  input.select(
    max("population").as("population"),
    avg("population")
  ).show()

  //14
  val nums = spark.range(5).withColumn("group", expr("id % 2"))
  nums.groupBy("group").agg(max("id").alias("max_id")).show

  //15
  nums.groupBy("group").agg(collect_list("id").alias("ids")).show

  //16
  nums
    .groupBy("group")
    .agg(
      max("id").alias("max_id"),
      min("id").alias("min_id")
    ).show
}
