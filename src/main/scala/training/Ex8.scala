package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

object Ex8 extends App {

  val spark = SparkSession
    .builder()
    .appName("ex8")
    .master("local[*]")
    .getOrCreate()

  val input = spark.read.format("csv")
    .option("inferSchema", true)
    .option("header", true)
    .load("/tmp/input/population.csv")

  val byCountry = Window.partitionBy(input("country")).orderBy(input("population").desc)

  import spark.sqlContext.implicits._

  val result = input
    .withColumn("rowNum", row_number() over byCountry)
    .select("name", "country", "population")
    .where($"rowNum" === 1)
    .sort("population")

  result.explain(true)
  result.show

  // Windowing

}
