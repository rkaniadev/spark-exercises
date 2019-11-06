package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Exercise [9, 12]
object Ex9_10_11_12 extends App {

  val spark = SparkSession
    .builder()
    .appName("ex9")
    .master("local[*]")
    .getOrCreate()

  // ex9

  import spark.sqlContext.implicits._

  // ex10
  val nums = Seq(Seq(1,2,3)).toDF("nums")
  val result = nums.withColumn("num", explode($"nums"))
  result.show()

  // ex11 dates1
  val dates = Seq(
    "08/11/2015",
    "09/11/2015",
    "09/12/2015").toDF("date_string")

  dates
    .withColumn("to_date", to_date($"date_string","dd/MM/YYYY"))
    .withColumn("diff", datediff(current_date(), $"to_date"))
    .show()

  // ex12
  val inputDF = Seq((1, "warsaw"), (2, "ottawa"), (3, "bratislava")).toDF("id", "city")
  inputDF.createTempView("cities")

  val upper: String => String = _.toUpperCase()
  val upperUDF = udf(upper)

  spark.udf.register("myUpper", (input: String) => input.toUpperCase())

  val upperCities = spark.sql("select myUpper(city) as upper_cities from cities")
  upperCities.explain(true)
  upperCities.show(false)

  val upperCities2 = ???

  // nondeterministic UDFs
  val my_date = udf { (n: Long) => util.Random.nextInt() }

}
