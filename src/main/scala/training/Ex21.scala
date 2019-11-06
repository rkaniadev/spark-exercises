package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_list, first}

object Ex21 extends App {

  val spark = SparkSession
    .builder()
    .appName("ex20")
    .master("local[*]")
    .getOrCreate()

  val input = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load("/tmp/input/ex21.csv")

  // grouped
  input.groupBy("key").agg(collect_list("val1"),
    collect_list("val2")).show()

  // how to alias
  input.groupBy("key")
    .pivot("date")
    .agg(
      first("val1").alias("v1"),
      first("val2").alias("v2")
    ).show()


}
