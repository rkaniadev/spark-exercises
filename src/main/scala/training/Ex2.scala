package training

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Ex2 {
  val log = Logger.getLogger(getClass)

  val spark = SparkSession
    .builder()
    .appName("exercise1")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    log.info(args)
    if (args.length > 0) readCSV(args(0)).show(false)
  }

  def readCSV(path: String) = {
    spark.read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load(path)
  }
}
