# Spark Streaming

流计算模式需要区分于静态数据，即实时计算与批量计算的区别。

### 流计算概述

静态数据在企业中是用于支持决策分析构建数据仓库系统的历史数据，数据使用ETL加载到数据仓库中，且不会发生更新。

流数据指的是时间分布和数量上无限的一系列动态数据集合体，数据记录是流数据的最小单元。特征如下：

1. 数据快速持续到达，潜在大小也许是无穷无尽的。

2. 数据来源多，格式复杂。

3. 数据量大，但不关注存储。=> 流数据某个元素经过处理要么被丢弃要么归档存储。

4. 关注整体价值而非个别数据。

5. 系统无法控制到达数据元素的顺序。

批量计算：以静态数据为对象，在充裕时间内对海量数据批量处理。

流数据必须采用实时计算，最重要的需求是实时得到计算结果，要求响应时间为秒级。

### 流计算概念

流计算基本理念是数据的价值随着时间流逝而降低。当事件出现就立即处理，而不是缓存起来批量处理。为了及时处理流数据，需要一个低延迟、可扩展、高可靠的处理引擎。

引擎要求有：高性能、海量式、实时性、分布式、易用性、可靠性。

### 流计算处理流程

1、数据实时采集

采集多个数据源的海量数据，需要保证实时性、低延迟与稳定可靠。

数据采集系统包含三个部分

1. Agent：主动采集数据，并把数据推动到Collector部分。

2. Collector：接收多个Agent的数据，实现有序、可靠、高性能的转发。

3. Store：存储Collector转发过来的数据。

2、数据实时计算

对采集的数据进行实时的分析和计算。经流处理处理后的数据可视情况进行存储。

3、实时查询服务

经由流计算框架得出结果可供用户进行实时查询、展示或存储。实时查询服务可以不断更新结果，并将用户所需结果实时推送给用户。

### Spark Streaming 设计

Spark Streaming 可整合多种数据源，经处理后的数据存储至文件系统、数据库中。

基本原理：将实时输入数据流以时间片为单位进行拆分，采用Spark引擎以类似批处理方式处理每个时间便数据。

Spark Streaming 主要的抽象是离散化数据流（DStream），表示连续不断的数据流。内部实现上，Spark Streaming 输入数据按时间片分成一段一段，每一段数据转换为Spark中的RDD，并对DStream操作变为RDD操作。

### DStream 操作概述

工作机制：

Spark Streaming中会有一个Reciver组件作为长期运行的Task运行在Executor中，每个Receiver负责一个DStream输入流。

程序执行步骤：

创建输入DStream定义输入源。

=> 产生的数据发送给Spark Streaming，由Reciever接收后交给程序处理。

=> 对DStream应用转换操作和输出操作定义流计算。

=> 调用StreamingContext对象start()方法开始接收数据和处理流程

=> 调用StreamingContext对象awaitTermination()方法等待流计算进程结束。

创建StreamingContext对象：

```scala
val ssc = new StreamingContext(conf,Second(1))
                            // Second(1) 表示数据流分段时每1秒切成一个分段
                            // conf 表示SparkConf相关配置
```

### 入门案例

```scala
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
```

### Spark Streaming 基本输入源

1、文件流

对文件系统某个目录进行监听，有新的文件生成就会自动把文件内容读取过来，使用用户自定义逻辑处理。

```scala
val line = ssc.textFileStream("file://path")
val wordCounts = line.flatmap(...).map(...).reduceByKey(...)
wordCounts.print()
ssc.start()                        //启动计算流程
ssc.awaitTermination()
```

2、套接字流

```scala
ssc.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)
/*
    参数： args(0) => 主机地址
          args(1) => 通信端口号
          StorageLevel.MEMORY_AND_DISK_SER => 使用内存和磁盘作为存储介质
*/
```

3、RDD队列流

```scala
val queueStream = ssc.queueStream(rddQueue)
```

案例

```scala
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
```

### 自定义数据源

```scala
package ac.sict.reid.leo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random


object SparkStreaming03_DIY {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming.md")
    val ssc = new StreamingContext(conf = conf, batchDuration = Seconds(3))

    /*
    * 主要任务
    */
    val messageDS = ssc.receiverStream(new MyReceiver())
    messageDS.print()

    ssc.start()
    ssc.awaitTermination()
  }

  /*
  * 自定义数据采集器
  * 1.自定义数据采集器，定义泛型
  * 2.重写方法
  * */
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){

    private var flg = true

    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while(flg){
            val message = "采集的数据为：" + new Random().nextInt(10).toString
            store(message)
            Thread.sleep(500)
          }
        }
      }).start()
    }

    override def onStop(): Unit = {
      flg = false
    }
  }

}
```