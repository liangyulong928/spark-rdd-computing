# Spark Streaming 的状态转换

对每个分段的DStream数据的处理就是对DStream的转换操作

### DStream 无状态转换操作

无状态转换不会记录历史状态信息，每次对新批次数据处理时，只记录当前批次数据状态。

常见无状态操作：

- map(function), flatMap(function), filter(function), repartition(numPartitions),

- reduce(func), count(), union(otherStream), countByValue(), reduceByKey(func, [numTasks])

- join(otherStream, [numTasks]), cogroup(otherStream, [numTasks]), transform(func)

### DStream 有状态转换

1、滑动窗口转换操作

    事先设定一个滑动窗口长度（窗口持续时间），设定滑动窗口时间间隔，让窗口按照时间间隔在DStream上滑动，每次窗口停放位置都有一部分DStream被框进窗口内，形成一小段DStream，启动对DStream计算（进行转换操作）。

相关操作

| 操作                                        | 含义                                    |
|:----------------------------------------- |:------------------------------------- |
| windows(length,slideInterval)             | 基于源DStream产生的窗口化的批数据，计算得到一个新的DStream  |
| countByWindow(length,slideInterval)       | 返回流中元素的一个滑动窗口数                        |
| reduceByWindow(func,length,slideInterval) | 返回一个单元素流。利用func对滑动窗口内元素进行聚集，得到一个单元素流。 |

示例代码

```scala
val wordCounts = pair.reduceByKeyAndWindow(_+_,_-_,Minutes(2),Seconds(10),2)
```

> 解释：
> 
> reduceByKeyAndWindow函数中，每个窗口reduce值是基于先前窗口的reduce值进行增量计算得到的。
> 
> 该函数会对进入滑动窗口的新数据进行reduce操作，对离开窗口的老数据逆向reduce操作。

2、updateStateByKey 操作

针对跨批次维护状态。该操作首先对DStream中数据根据key做计算，然后各批次数据进行累加。

对参数函数 func 有：(Seq[V], Option([S])  => Opyion[S]，其中Seq[V]表示key对应的所有value、Option[S]表示key的历史状态，返回值表示当前key的新状态。

示例代码

```scala
package ac.sict.reid.leo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCountStateful {
  def main(args: Array[String]): Unit = {
    val updateFunc = (values:Seq[Int],state:Option[Int])=> {
      val currentCount = values.foldLeft(0)(_ + _)
                    // 对当前key对应value-list进行汇总求和
      val previousCount = state.getOrElse(0)
                    // 对当前key获取对应历史信息
      Some(currentCount + previousCount)
    }
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCountStateful")
    val sc = new StreamingContext(conf = conf, batchDuration = Seconds(5))

    sc.checkpoint("checkpoint")

    val lines = sc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordDStream = words.map(x => (x, 1))
    val stateDStream = wordDStream.updateStateByKey[Int](updateFunc)
    // updateStateByKey 会把所有key相同的(key，value)进行归并，得到(key，value-list)形式
    stateDStream.print()
    sc.start()
    sc.awaitTermination()
  }
}
```

# Spark Streaming 的输出操作

输出到文本文件

```scala
stateDStream.saveAsTextFiles("file://path")
```

写入关系型数据库

```scala
stateDStream.foreachRDD(rdd=>{
  def func(records: Iterator[(String, Int)]){
    var conn : Connection = null
    var stmt : PerparedStatement = null
    try{
      val url = "jdbc-driver"
      val user = "user"
      val password = "passwd"
      conn = DriverManager.getConnection(url,user,password)
      records.foreach(p=>{
        val sql = "insert into wordcount(word,count) values (?,?)"
        stmt = conn.prepareStatement(sql);
        stmt.setString(1,p._1.trim)
        stmt.setInt(2,p._2.toInt)
        stmt.executeUpdate()
      })
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      if (stmt != null){
        stmt.close()
      }
      if(conn != null){
        conn.close()
      }
    }
  }
  val repartitionedRDD = rdd.repartition(3)
  repartitionedRDD.foreachPartition(func)
})
```
