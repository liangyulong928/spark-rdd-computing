package ac.sict.reid.leo.rdd_program_primary

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object AverageScore {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Marge")
    val sc = new SparkContext(sparkConf)
    val dataRDD: RDD[String] = sc.textFile("data/score/")
    val scoreRDD = dataRDD.map(line => {
      val strings = line.split(" ")
      (strings(0), strings(1).toFloat)
    })
    val numRDD = dataRDD.map(line => {
      val strings = line.split(" ")
      (strings(0), 1)
    })
    val sumRDD = scoreRDD.reduceByKey((x, y) => x + y)
    val countRDD = numRDD.reduceByKey((x, y) => x + y)
    val valueRDD = sumRDD.join(countRDD)
    val avgRDD = valueRDD.mapValues{
      case(value,count) => value / count
    }
    avgRDD.collect().foreach(println)
  }
}
