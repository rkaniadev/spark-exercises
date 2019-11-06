package training

import org.apache.spark.sql.SparkSession

object Ex4 extends App {

  val spark = SparkSession
    .builder()
    .appName("exercise1")
    .master("local[*]")
    .getOrCreate()

  // import spark.sqlContext.implicits._
  import spark.sqlContext.implicits._
  val nums = Seq(Seq(1,2,3)).toDF("nums").as[Array[Int]]

  // TODO ???

}
