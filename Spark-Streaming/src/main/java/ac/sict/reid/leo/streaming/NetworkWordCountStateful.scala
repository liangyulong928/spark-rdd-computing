package ac.sict.reid.leo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.DriverManager

object NetworkWordCountStateful {
  def main(args: Array[String]): Unit = {
    val updateFunc = (values:Seq[Int],state:Option[Int])=> {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCountStateful")
    val sc = new StreamingContext(conf = conf, batchDuration = Seconds(5))

    // TODO
    sc.checkpoint("checkpoint")

    val lines = sc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordDStream = words.map(x => (x, 1))
    val stateDStream = wordDStream.updateStateByKey[Int](updateFunc)
    stateDStream.print()
    sc.start()
    sc.awaitTermination()
  }

}