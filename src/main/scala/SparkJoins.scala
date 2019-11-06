import org.apache.spark.sql.SparkSession

object SparkJoins extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("join-types")
    .getOrCreate()

  import spark.sqlContext.implicits._

  val simpleDF1 = Seq(
    (1, "foo", 12),
    (2, "bar", 13),
    (3, "null", 0)
  ).toDF("id", "name", "descId")

  val simpleDF2 = Seq(
    (12, "is a simple scala function"),
    (13, "is a value - immutable"),
    (14, "expression")
  ).toDF("id", "desc")

  // outer || full || fullouter
  simpleDF1
    .join(simpleDF2, simpleDF1("descId") === simpleDF2("id"), "outer")
    .show

//  +---+----+------+----+--------------------+
//  | id|name|descId|  id|                desc|
//  +---+----+------+----+--------------------+
//  |  1| foo|    12|  12|fuu is a simple s...|
//  |  2| bar|    13|  13|bar is a value - ...|
//  |  3|null|     0|null|                null|
//  +---+----+------+----+--------------------+

  // inner
  simpleDF1
    .join(simpleDF2, simpleDF1("descId") === simpleDF2("id"), "inner")
    .show

//  +---+----+------+---+--------------------+
//  | id|name|descId| id|                desc|
//  +---+----+------+---+--------------------+
//  |  1| foo|    12| 12|fuu is a simple s...|
//  |  2| bar|    13| 13|bar is a value - ...|
//  +---+----+------+---+--------------------+

  // leftanti
  // get from left side all which are not related with any from right

  simpleDF1
    .join(simpleDF2, simpleDF1("descId") === simpleDF2("id"), "leftanti")
    .show

//  +---+----+------+
//  | id|name|descId|
//  +---+----+------+
//  |  3|null|     0|
//  +---+----+------+

  // leftouter || left
  simpleDF1
    .join(simpleDF2, simpleDF1("descId") === simpleDF2("id"), "left")
    .show

//  +---+----+------+----+--------------------+
//  | id|name|descId|  id|                desc|
//  +---+----+------+----+--------------------+
//  |  1| foo|    12|  12|fuu is a simple s...|
//  |  2| bar|    13|  13|bar is a value - ...|
//  |  3|null|     0|null|                null|
//  +---+----+------+----+--------------------+

  // leftsemi
  simpleDF1
    .join(simpleDF2, simpleDF1("descId") === simpleDF2("id"), "leftsemi")
    .show

//  +---+----+------+
//  | id|name|descId|
//  +---+----+------+
//  |  1| foo|    12|
//  |  2| bar|    13|
//  +---+----+------+

  // right || right outer
  simpleDF1
    .join(simpleDF2, simpleDF2("id") === simpleDF1("descId"), "right")
    .show

//  +----+----+------+---+--------------------+
//  |  id|name|descId| id|                desc|
//  +----+----+------+---+--------------------+
//  |   1| foo|    12| 12|is a simple scala...|
//  |   2| bar|    13| 13|is a value - immu...|
//  |null|null|  null| 14|          expression|
//  +----+----+------+---+--------------------+

}

case class Person(id: Long, name: String)
