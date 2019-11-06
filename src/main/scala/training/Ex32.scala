package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

object Ex32 extends App {

  val spark = SparkSession
    .builder()
    .appName("ex9")
    .master("local[*]")
    .getOrCreate()

  import spark.sqlContext.implicits._

  val input = Seq(
    (1, "Mr"),
    (1, "Mme"),
    (1, "Mr"),
    (1, null),
    (1, null),
    (1, null),
    (2, "Mr"),
    (3, null)
  ).toDF("UNIQUE_GUEST_ID", "PREFIX")

  val byUniqueId = Window.partitionBy("UNIQUE_GUEST_ID")

  byUniqueId

}