//Which IP accesses the website most? How many accesses were made by it?

import org.apache.spark.{SparkConf, SparkContext}

object IPCount{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("IPCount")
    val sc = SparkContext.getOrCreate(conf)

    val lines = sc.textFile("hdfs://master:9000/user/root/miniProjectI_2/access_log")
    val IPPattern = "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))".r
    val ipNums = lines.flatMap(x=>IPPattern findFirstIn(x)).map(x=>(x,1)).reduceByKey((x,y)=>x+y).sortBy(_._2,false)
    ipNums.take(1).foreach(println)
  }
}