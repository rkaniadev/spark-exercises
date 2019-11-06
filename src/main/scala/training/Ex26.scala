package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws}

object Ex26 extends App {

  val spark = SparkSession
    .builder()
    .appName("ex9")
    .master("local[*]")
    .getOrCreate()

  import spark.sqlContext.implicits._
  val words = Seq(Array("hello", "world")).toDF("words")

  words.select(concat_ws(" ", col("words")).alias("solution")).show
}
