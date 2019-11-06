package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, first, lit}

object Ex20 extends App {

  val spark = SparkSession
    .builder()
    .appName("ex20")
    .master("local[*]")
    .getOrCreate()

  val input = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load("/tmp/input/ex20.csv")

  input
    .groupBy("ParticipantID", "Assessment", "GeoTag")
    .pivot(concat(lit("Quid_"), col("Qid")))
    .agg(first("AnswerText"))
    .show()

}
