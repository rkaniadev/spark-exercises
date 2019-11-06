package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object Ex17_18_19 extends App {

  val spark = SparkSession
    .builder()
    .appName("ex9")
    .master("local[*]")
    .getOrCreate()

  val input = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load("/tmp/input/ex17.csv")

  input.show

  import spark.implicits._

  // 17
  println("debug")
  input.withColumn("tmp", lit("cc"))
    .groupBy("tmp")
    .pivot("udate")
    .agg(first("cc"))
    .show()


  // 18 Pivot with agg
  // pivot allows only to pivot on one column
  val data = Seq(
    (0, "A", 223, "201603", "PORT"),
    (0, "A", 22, "201602", "PORT"),
    (0, "A", 422, "201601", "DOCK"),
    (1, "B", 3213, "201602", "DOCK"),
    (1, "B", 3213, "201601", "PORT"),
    (2, "C", 2321, "201601", "DOCK")
  ).toDF("id","type", "cost", "date", "ship")
    .withColumn("cost", col("cost").cast(IntegerType) )

  val dates = Seq("201601", "201602", "201603")

  data
    .groupBy("id", "type")
    .pivot("date", dates)
    .max("cost")
    .orderBy("id").show

  data
    .groupBy("id", "type")
    .pivot("date", dates)
    .agg(collect_list("ship"))
    .orderBy("id")
    .show

  // 19 pivoting on multiple columns
  val data2 = Seq(
    (100,1,23,10),
    (100,2,45,11),
    (100,3,67,12),
    (100,4,78,13),
    (101,1,23,10),
    (101,2,45,13),
    (101,3,67,14),
    (101,4,78,15),
    (102,1,23,10),
    (102,2,45,11),
    (102,3,67,16),
    (102,4,78,18)).toDF("id", "day", "price", "units")

  data2.cache()

  val part1 = data2.groupBy("id")
    .pivot(concat(lit("price_"), col("day")))
    .agg(first("price"))
    .orderBy("id")

  part1.show()

  val part2 = data2.groupBy("id")
    .pivot(concat(lit("unit_"), col("day")))
    .agg(first("units"))
    .orderBy("id")

  part2.show()

  part1
    .join(part2, part2("id") === part1("id"))
    .select(
      part1("id"),
      part1("price_1"),
      part1("price_2"),
      part1("price_3"),
      part1("price_4"),
      part2("unit_1"),
      part2("unit_2"),
      part2("unit_3"),
      part2("unit_4")
    ).show

}
