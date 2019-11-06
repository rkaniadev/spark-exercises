package training

import org.apache.spark.sql.SparkSession

object Ex5 extends App {

  val spark = SparkSession
    .builder()
    .appName("")
    .master("local[*]")
    .getOrCreate()

  val df = spark.read
    .option("delimiter","|")
    .option("comment", "+")
    .option("header", true)
    .csv("/tmp/input/reverse.txt")


  df.show(false)
}
