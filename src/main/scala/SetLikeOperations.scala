import org.apache.spark.sql.SparkSession

object SetLikeOperations extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("join-types")
    .getOrCreate()

  import spark.sqlContext.implicits._

  val df1 = Seq(
    (101, "Terminal 1",1001,"2019-06-25",null),
    (102, "Terminal 2",1002,"2019-06-25",null),
    (103, "Terminal 3",1002,"2019-06-25",null)
  ).toDF("id","name","shopID","activeFrom","activeUpTo").as[TerminalLocation]

  val df2 = Seq(
    (101, "Terminal 1", 1001,"2019-06-25",null),
    (102, "Terminal 2", 1003,"2019-06-25",null),
    (103, "Terminal 3",1002,"2019-06-25",null),
    (104, "Terminal 4", 1002,"2019-06-25",null)
  ).toDF("id","name","shopId","activeFrom","activeUpTo").as[TerminalLocation]

  // set ++ set
  df1.union(df2).show()
  df1.intersect(df2).show()

  val tl1 = TerminalLocation(1, "Terminal 3", 1002, "2019-06-25", null)
  val tl2 = TerminalLocation(1, "Terminal 3", 1002, "2019-06-25", null)
  assert(tl1 == tl2)

  Seq(1,2,3).toDF().intersect(Seq(3,4,5).toDF()).show()
}

case class TerminalLocation(id: Long, name: String, shopId: Long, activeFrom: String, activeUpTo: String)