# RDD编程初级实验记录

实验时间：2023年8月18日

实验地点：sict-reid

#### 1、spark-shell交互式编程

```scala
scala> val rdd = sc.textFile("chapter5-data1.txt")
rdd: org.apache.spark.rdd.RDD[String] = chapter5-data1.txt MapPartitionsRDD[1] at textFile at <console>:23

scala> val dataRdd = rdd.map(_.split(","))
dataRdd: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[2] at map at <console>:23

scala> val stuRdd = dataRdd.map(t=>(t(0),1))
stuRdd: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[3] at map at <console>:23

scala> val stuCountRdd = stuRdd.reduceByKey(_+_)
stuCountRdd: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[4] at reduceByKey at <console>:23

scala> stuCountRdd.collect().foreach(println)
(Bartholomew,5)
(Lennon,4)
(Joshua,4)
(Tom,5)
...
(Winfred,3)
(Lionel,4)
(Bob,3)

scala> stuCountRdd.count()
res1: Long = 265

scala> val courseRdd = dataRdd.map(t=>(t(1),1))
courseRdd: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[5] at map at <console>:2
3

scala> val courseCountRdd = courseRdd.reduceByKey(_+_)
courseCountRdd: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[6] at reduceByKey at <console>:23

scala> courseCountRdd.collect().foreach(println)
(Python,136)
(OperatingSystem,134)
(CLanguage,128)
(Software,132)
(Algorithm,144)
(DataStructure,131)
(DataBase,126)
(ComputerNetwork,142)

scala> courseCountRdd.count()
res3: Long = 8


scala> val TomScoreRdd = dataRdd.filter(t=>(t(0)=="Tom")).map(t=>(t(0),t(2).toInt))
TomScoreRdd: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[11] at map at <console>:23

scala> val TomAvgScore = TomScoreRdd.reduceByKey(_+_)
TomAvgScore: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[12] at reduceByKey at <console>:23

scala> val TomCountScore = stuCountRdd.filter{case(a,b) => a=="Tom"}
TomCountScore: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[8] at filter at <console>:23


scala> val TomAvgRdd = TomAvgScore.join(TomCountScore)
TomAvgRdd: org.apache.spark.rdd.RDD[(String, (Int, Int))] = MapPartitionsRDD[11] at join at <console>:24

scala> TomAvgRdd.collect().foreach(println)
(Tom,(154,5))


scala> val res = TomAvgRdd.mapValues{ case (a,b) => a.toFloat / b}
res: org.apache.spark.rdd.RDD[(String, Float)] = MapPartitionsRDD[13] at mapValues at <console>:23

scala> res.collect().foreach(println)
(Tom,30.8)

scala> stuCountRdd.collect().foreach(println)
(Bartholomew,5)
(Lennon,4)
(Joshua,4)
(Tom,5)
...
(Winfred,3)
(Lionel,4)
(Bob,3)

scala> val dbRdd = dataRdd.filter(t=>t(1)=="DataBase")
dbRdd: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[14] at filter at <console>:23

scala> val dbCountRdd = dbRdd.map(t=>(t(1),1)).reduceByKey(_+_)
dbCountRdd: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[16] at reduceByKey at <console>:23

scala> dbCountRdd.collect().foreach(println)
(DataBase,126)

scala> val courseScoreRdd = dataRdd.map(t=>(t(1),t(2).toFloat)).reduceByKey(_+_)
courseScoreRdd: org.apache.spark.rdd.RDD[(String, Float)] = ShuffledRDD[23] at reduceByKey at <console>:23

scala> val courseCountRdd = dataRdd.map(t=>(t(1),1)).reduceByKey(_+_)
courseCountRdd: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[25] at reduceByKey at <console>:23

scala> val courseAvgRdd = courseScoreRdd.join(courseCountRdd)
courseAvgRdd: org.apache.spark.rdd.RDD[(String, (Float, Int))] = MapPartitionsRDD[28] at join at <console>:24


scala> courseAvgRdd.collect().foreach(println)
(Python,(7864.0,136))
(OperatingSystem,(7362.0,134))
(CLanguage,(6478.0,128))
(Software,(6720.0,132))
(Algorithm,(7032.0,144))
(DataStructure,(6232.0,131))
(DataBase,(6368.0,126))
(ComputerNetwork,(7370.0,142))


scala> val courseAvgRes = courseAvgRdd.mapValues{case (a,b) => a/b}
courseAvgRes: org.apache.spark.rdd.RDD[(String, Float)] = MapPartitionsRDD[30] at mapValues at <console>:23

scala> courseAvgRes.collect().foreach(println)
(Python,57.82353)
(OperatingSystem,54.9403)
(CLanguage,50.609375)
(Software,50.909092)
(Algorithm,48.833332)
(DataStructure,47.572517)
(DataBase,50.539684)
(ComputerNetwork,51.90141)
```

#### 2、编写独立应用程序实现数据去重

对输入文件A和B进行合并，剔除重复内容

1、读取文件

```scala
val dataRDD: RDD[String] = sc.textFile("data/marge/")
//在本部分中通过写明文件夹路径，即可在创建该RDD时读取文件夹所有文件
```

2、数据去重

```scala
val value = dataRDD.distinct().sortBy(x=>x)
//使用RDD的distinct方法可以实现去重操作，而后使用sortBy对其进行排序
```

#### 3、编写独立应用程序实现求平均值

要求：将“学生名字 成绩“的信息输入并计算所有学生平均成绩

1、获取学生总成绩

```scala
    val scoreRDD = dataRDD.map(line => {
      val strings = line.split(" ")
      (strings(0), strings(1).toFloat)
    })
    val sumRDD = scoreRDD.reduceByKey((x, y) => x + y)
```

2、获取学生获得成绩科目个数

```scala
    val numRDD = dataRDD.map(line => {
      val strings = line.split(" ")
      (strings(0), 1)
    })
    val countRDD = numRDD.reduceByKey((x, y) => x + y)
```

3、通过“总成绩/学科数“计算平均成绩

```scala
    val valueRDD = sumRDD.join(countRDD)
    val avgRDD = valueRDD.mapValues{
      case(value,count) => value / count
    }
```

> 存在问题：
> 
> 浮点数未解决小数点问题，需要保留小数点后两位
