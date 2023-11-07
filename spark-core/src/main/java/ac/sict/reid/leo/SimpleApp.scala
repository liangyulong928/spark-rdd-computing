package ac.sict.reid.leo

import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val logFile = "file:///Users/leostudio/Documents/Code/Data/RDD城市关注度实验记录.md"
    val conf = new SparkConf().setMaster("local[*]").setAppName("SimpleApp")
    val sc = new SparkContext(conf)
    val value = sc.textFile(logFile, 2).cache()
    val la = value.filter(line => line.contains("a")).count()
    val lb = value.filter(line => line.contains("b")).count()
    println("a: %s, b: %s".format(la,lb))
    sc.stop()
  }

}
