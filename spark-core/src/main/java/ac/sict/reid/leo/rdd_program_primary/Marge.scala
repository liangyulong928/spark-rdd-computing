package ac.sict.reid.leo.rdd_program_primary

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Marge {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Marge")
    val sc = new SparkContext(sparkConf)
    val dataRDD: RDD[String] = sc.textFile("data/marge/")
    val value = dataRDD.distinct().sortBy(x=>x)
    value.collect().foreach(println)
    sc.stop()
  }

}
