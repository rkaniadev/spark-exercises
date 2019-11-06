package training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Ex30 extends App {

  val spark = SparkSession
    .builder()
    .appName("ex9")
    .master("local[*]")
    .getOrCreate()

  import spark.sqlContext.implicits._

  val data = Seq(
    (None, 0),
    (Some(3), 1),
    (Some(2), 0),
    (None, 1),
    (Some(4), 1)).toDF("id", "group")

//  val w1 = Window
//    .partitionBy("group")
//    .orderBy("id")
//    .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

  val withoutNulls = data
    .groupBy("group").agg(first("id", ignoreNulls = true))

  withoutNulls.show()
}
