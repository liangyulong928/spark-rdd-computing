package ac.sict.reid.leo.fake

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 实现各个城市关注的排行榜
 * 数据格式：时间戳，用户所在省份，用户关注城市，用户id
 */
object FakeCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("FakeCount")
    val sc = new SparkContext(conf)
    val dataRdd = sc.textFile("data/output.log")
    val mapRDD = dataRdd.map(
      line => {
        val strings = line.split(" ")
        ((strings(2), strings(3)), 1)
      }
    )
    val reduceRDD: RDD[((String,String),Int)] = mapRDD.reduceByKey(_+_)
    val newMapRDD = reduceRDD.map {
      case ((local, attention), num) => {
        (attention, (local, num))
      }
    }
    val groupRDD: RDD[(String,Iterable[(String,Int)])] = newMapRDD.groupByKey()
    val resultRDD = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse)
      }
    )
    resultRDD.collect().foreach(println)
    sc.stop()
    


  }

}
