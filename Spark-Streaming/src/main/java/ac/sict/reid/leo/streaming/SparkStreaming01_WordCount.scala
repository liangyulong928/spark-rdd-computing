package ac.sict.reid.leo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    /*
    * streamingContext参数：
    *       第一个参数表示环境配置
    *       第二个参数表示采集周期
    * */
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming.md")
    val ssc = new StreamingContext(conf = conf, batchDuration = Seconds(3))
    //TODO 逻辑处理
    /*
    * 获取端口数据
    * */
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val word2One = words.map((_, 1))
    val word2Count = word2One .reduceByKey(_ + _)
    word2Count.print()
    //TODO 关机环境(采集器长期执行，不能直接关闭)
    /*
    * 如果main方法执行完毕，应用程序会自动结束，因此不能让main方法执行完毕
    *
    * 如何操作？
    *     1、启动采集器
    *     2.等待采集器关闭
    * */
    ssc.start()
    ssc.awaitTermination()
    //    ssc.stop()
  }
}
