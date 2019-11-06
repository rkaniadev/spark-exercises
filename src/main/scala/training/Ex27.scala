package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{percent_rank, when}

// widowing
// percent_rank
// when, otherwise
object Ex27 extends App {

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
    .load("/tmp/input/ex27.csv")

  import spark.sqlContext.implicits._

  val w1 = Window.orderBy("salary")

  val result = input.withColumn("percentage", percent_rank() over w1)
    .withColumn("percentage",
      when($"percentage" > 0.7, "High")
        .when($"percentage" < 0.3, "Low")
        .otherwise("Average")
    )

  result.show

}
