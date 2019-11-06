package slow.changing.dim

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
 * Problem: Slowly changing dimensions type 2.
 * Current dataset stores actual records and historical data.
 */
object Main extends App {
  // Slowly changing dimensions - type2
  // full history of values
  val spark = SparkSession
    .builder()
    .appName("slowly-changing-dimensions-type-2")
    .master("local[*]")
    .getOrCreate()

  // depends on data source
  // filtering etc, for example if source is partitioned
  // we can do things to load only partitions that we need
  // bucketing etc...
  import spark.sqlContext.implicits._

  val currentDF = Seq(
    (101, "Terminal 1",1001,"2019-06-25",null),
    (102, "Terminal 2",1002,"2019-06-25",null),
    (103, "Terminal 3",1002,"2019-06-25",null)
  ).toDF("ID","Name","shopID","activeFrom","activeUpTo")

  currentDF.repartition(3)

  val newDF = Seq(
    (101, "Terminal 1", 1001),
    (102, "Terminal 2", 1003),
    (104, "Terminal 4", 1002)
  ).toDF("ID","name","shopId")

  currentDF.repartition(3)

  def withHash(df: DataFrame): DataFrame = {
    df.withColumn("hash", concat(col("ID"), col("Name"), col("shopId")))
      .withColumn("hash", sha1(col("hash")))
  }

  val hashedCurrent = withHash(currentDF)
  val hashedNew = withHash(newDF)

  hashedCurrent.show(false)
  hashedNew.show(false)

  // Select unchanged
  def getUnchanged(currentDF: DataFrame, newDF: DataFrame): DataFrame = {
    currentDF.as("current")
      .join(newDF.as("new"), currentDF("hash") === newDF("hash"), "inner")
      .select("current.*")
      .drop("hash")
  }

  // Select
  def getNew(currentDF: DataFrame, newDF: DataFrame): DataFrame = {
    newDF.as("new")
      .join(currentDF.filter(col("activeUpTo").isNull).as("current"),
        currentDF("hash") === newDF("hash"), "leftanti")
      .select("new.*")
      .withColumn("activeUpTo", lit(null).cast(StringType))
      .withColumn("activeFrom", current_date().cast(StringType))
      .drop("hash")
  }

  def getObsolete(currentDF: DataFrame, newDF: DataFrame): DataFrame = {
    currentDF.as("current")
      .join(newDF.as("new"), currentDF("id") ===  newDF("id"), "leftanti")
      .select("current.*")
      .drop("hash")
  }

  // in current
  // in new
  val unchangedRecords = getUnchanged(hashedCurrent, hashedNew)
  unchangedRecords.show
  // not in current
  // in new
  val newRecords = getNew(hashedCurrent, hashedNew)
  newRecords.show
  // in current
  // not in new
  val deletedRecords = getObsolete(hashedCurrent, hashedNew)
  deletedRecords.show

  val result = unchangedRecords
    .union(newRecords)
    .union(deletedRecords)

  result.show(false)
  result.explain(true)
  println(result.rdd.toDebugString)
}