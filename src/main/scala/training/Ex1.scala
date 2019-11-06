package training

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Ex1 {
  val log = Logger.getLogger(getClass)

  val spark = SparkSession
    .builder()
    .appName("exercise1")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    import spark.sqlContext.implicits._

    val input = for (i <- 1 to 1000000) yield (i, i % 8)
    val df: DataFrame = input.toDF("value", "index")
    val grouped = df
      .map{ case Row(v: Int, i: Int) => (v*2, i) }

    grouped.explain(true)

//    val count = grouped.count().collect()
//    log.info(count)

    Thread.sleep(50000)
  }
}
