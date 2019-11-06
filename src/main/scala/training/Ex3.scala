package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_list

object Ex3 extends App {

  val spark = SparkSession
    .builder()
    .appName("exercise1")
    .master("local[*]")
    .getOrCreate()

  val inputDF = spark.read
    .format("csv")
    .option("inferSchema", true)
    .option("header", true)
    .load("c:\\workplace\\data\\input\\exercise3.csv")

  inputDF.show()

  val words = inputDF.select("word")
  val idWithWords = inputDF.select("id", "words")

  words.show()
  idWithWords.show()

  val grouped = words.join(idWithWords, idWithWords("words").contains(words("word")))
    .select("id", "word")
    .groupBy("word").agg(collect_list("id") as "Ids")
    .withColumnRenamed("word", "w")
    .sort("w")

  grouped.explain(true)
  grouped.show()
}
