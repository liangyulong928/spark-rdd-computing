package ac.sict.reid.leo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


object SparkStreaming02_RDDQueue {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming.md")
    val ssc = new StreamingContext(conf = conf, batchDuration = Seconds(3))

    val rdd = new mutable.Queue[RDD[Int]]()
    val inputStream = ssc.queueStream(rdd, oneAtATime = false)
    val mappedStream = inputStream.map((_, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    reducedStream.print()

    ssc.start()
    for (i <- 1 to 5){
      rdd += ssc.sparkContext.makeRDD(1 to 300,10)
      Thread.sleep(2000)
    }
    ssc.awaitTermination()
  }

}