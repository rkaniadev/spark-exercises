package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.element_at

object Ex6 extends App {

  val spark = SparkSession
    .builder()
    .appName("ex6")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val input = Seq(
    Seq("a", "b", "c"),
    Seq("X", "Y", "Z")
  ).toDF.as[Array[String]]

  input.show(false)

  // really bad approach ???
  val output = input
    .withColumn("0", element_at(input("value"), 1))
    .withColumn("1", element_at(input("value"), 2))
    .withColumn("2", element_at(input("value"), 3))
    .drop("value")

  output.show(false)
  output.printSchema()
  output.explain(true)

  // pivot
  // rotate data from one column into multiple columns

  val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
    ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
    ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

  val df = data.toDF("Product","Amount","Country")
  df.show()

  // pivot how to use
  val pivotDf = df
    .groupBy("Product")
    .pivot("Country")
    .sum("Amount")

  pivotDf.explain(true)
//  pivotDf.show(false)

  // two-phase aggregation
  val pivot2Df = df
    .groupBy("Product", "Country")
    .sum("Amount")
    .groupBy("Product")
    .pivot("Country")
    .sum("sum(Amount)")

  pivot2Df.explain(false)

  //  pivot2Df.show(true)

}
