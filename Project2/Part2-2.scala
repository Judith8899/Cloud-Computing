//How many hits were made to the website item “/assets/js/lightbox.js”?
import org.apache.spark.{SparkConf, SparkContext}

object HitCount2{
  def main(args: Array[String]): Unit = {
    def conf = new SparkConf().setAppName("WordCountSorted").setMaster("local")
    val sc = SparkContext.getOrCreate(conf)

    val lines = sc.textFile("hdfs://master:9000/user/root/miniProjectI_2/access_log")
    val words = lines.flatMap(_.split("/n")).filter((word => word.contains("/assets/js/lightbox.js")))
    val pairs = words.map(word => ("/assets/js/lightbox.js",1))
    val wordcount = pairs.reduceByKey(_+_)

    wordcount.collect.foreach(println)
  }
}