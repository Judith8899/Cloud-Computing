//How many hits were made to the website item “/assets/img/loading.gif”?
import org.apache.spark.{SparkContext, SparkConf}

object HitCount1{
  def main(args: Array[String]): Unit = {
    def conf = new SparkConf().setMaster("local").setAppName("WordCountSorted")
    val sc = SparkContext.getOrCreate(conf)

    val lines = sc.textFile("hdfs://master:9000/user/root/miniProjectI_2/access_log")
    val words = lines.flatMap(_.split("/n")).filter((word => word.contains("/assets/img/loading.gif")))
    val pairs = words.map(word => ("/assets/img/loading.gif",1))
    val wordcount = pairs.reduceByKey(_+_)

    wordcount.collect.foreach(println)
  }
}