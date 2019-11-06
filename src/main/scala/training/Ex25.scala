package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Ex25 extends App {

  val spark = SparkSession
    .builder()
    .appName("ex9")
    .master("local[*]")
    .getOrCreate()

  // ex. 24
  val input = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load("/tmp/input/ex25.csv")

  import spark.sqlContext.implicits._

  val byDepartment = Window.partitionBy("department").orderBy(asc("running_total"))
  val result = input
    .withColumn("diff", $"running_total" - (lag($"running_total", 1, 0) over byDepartment))

  result.show()
}
