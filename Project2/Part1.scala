//The  task  is  to  printout  the  total  listening  counts  of  each  artist.
// Your  program  should  print  out  the  listening  counts  per  artist  in  descending  order.
// In  the  ‘user_artists.dat’  file,  listening  counts  for  each user?artist pair is indicated by the variable ‘weight’.

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object Count {
  val spark = SparkSession.builder().master("yarn").appName("Listen Count").getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  val sschema = StructType(Array(
    StructField("uid", DoubleType),
    StructField("aid", DoubleType),
    StructField("weight", DoubleType)
  ))
  val Score = spark.read.schema(sschema).csv("hdfs://master:9000/user/root/miniProjectII/user_artists.csv")
  val AggScore = Score.groupBy('aid).agg(sum('weight) as "total_weight")
  val AggScoreOrder = AggScore.sort(desc("total_weight"))

}
Count.AggScoreOrder.show()

spark.stop()