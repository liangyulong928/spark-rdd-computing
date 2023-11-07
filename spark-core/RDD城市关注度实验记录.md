# Spark框架RDD算子实操

项目名称：使用RDD算子在日志文件中找到不同省份的人对各城市关注度的排行

实验时间：2023年8月18日

实验地点：sict-reid

## 实验数据分析

实验数据包含日期、时间、用户所在省份、用户关注城市、用户ID，格式如下

```json
1992-07-14 03:57:05 广东省 上海市 xiulan39
```

在本次实验中，我们只需关注“用户所在省份、用户关注城市”即可。

### 实验思路

1、以（用户所在省份，用户关注城市）作为Key进行Count，统计中每个省份对不同城市的关注度。

2、将用户关注城市作为Key，将（用户所在成分，关注度值）作为value生成迭代器

3、将迭代器数据进行降序排序并打印输出

### 实验核心代码

从文件中获取数据

```scala
val dataRdd = sc.textFile("data/output.log")
```

> 本次文件读取相对路径本地文件

数据清洗，获得（key，1）结构

```scala
val mapRDD = dataRdd.map(
    line => {
        val strings = line.split(" ")
        ((strings(2), strings(3)), 1)
    }
)
```

将key做数值聚合，获取各省份对不同城市的关注度

```scala
val reduceRDD: RDD[((String,String),Int)] = mapRDD.reduceByKey(_+_)
```

对该中间结果进行调整（采用模式匹配方式） 

```scala
val newMapRDD = reduceRDD.map {
    case ((local, attention), num) => {
        (attention, (local, num))
    }
}
```

对关注城市key做聚合，形成迭代器

```scala
val groupRDD: RDD[(String,Iterable[(String,Int)])] = newMapRDD.groupByKey()
```

对迭代器进行排序处理

```scala
val resultRDD = groupRDD.mapValues(
    iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse)
    }
)
```

### 实验结果

```shell
(天津市,List((北京市,26), (广东省,24), (上海市,23), (重庆市,23), (江苏省,23), (浙江省,18), (天津市,14)))
(深圳市,List((上海市,25), (北京市,23), (浙江省,21), (江苏省,20), (天津市,20), (重庆市,19), (广东省,19)))
(重庆市,List((浙江省,21), (江苏省,21), (广东省,16), (重庆市,16), (上海市,15), (北京市,15), (天津市,15)))
(杭州市,List((浙江省,27), (江苏省,26), (北京市,22), (广东省,21), (天津市,18), (上海市,18), (重庆市,17)))
(广州市,List((重庆市,23), (上海市,22), (广东省,21), (北京市,19), (天津市,18), (浙江省,18), (江苏省,16)))
(北京市,List((江苏省,31), (北京市,25), (重庆市,25), (上海市,20), (浙江省,17), (天津市,17), (广东省,15)))
(上海市,List((广东省,29), (天津市,28), (重庆市,24), (江苏省,22), (上海市,22), (北京市,17), (浙江省,5)))
```
