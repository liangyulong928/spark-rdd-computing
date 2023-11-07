# Spark SQL

Spark SQL在Hive兼容层面仅依赖于HiveQL解析和Hive原数据。通过使用DataFrame（带有Schema信息的RDD）使用户执行SQL语句。

### DataFrame

DataFrame是一种以RDD为基础的分布式数据集，提供了详细的结构信息，相当于关系数据库的一张表。每个RDD元素都是一个Java对象，使用DataFrame时，对象内部结构也可以看到（字段及其数据类型）。

##### DataFrame创建

通过创建SparkSession对象将从数据源加载数据并将数据转换成DataFrame。通过使用spark.read操作可以从不同文件中加载数据创建DataFrame（此时的spark指的是通过SparkSession.builder().getOrCreate()创建的对象）。

```scala
val df = spark.read.json("people.json")
//读取json数据

df.show()    //展示记录
```

##### DataFrame保存

通过 spark.write 操作将 DataFrame 保存成不同格式的文件。

```scala
val df = spark.read.json("people.json")
df.select("name","age").write.format("csv")    
            //选择name和age两个列属性数据保存起来

df.rdd()    //将DataFrame保存为文本文件
```

##### DataFrame常见操作

```scala
df.printSchema()
            //打印DataFrame模式信息
df.select()
            //选择部分列的数据
df.select(df("name").as("username"),df("age")).show()
            //将name列重命名为username
df.filter(df("age") > 20).show()
            //查询所有age大于20的记录
df.groupBy("age").count().show()
            //对age字段分组，统计分组中包含的记录数量
df.sort(df("age").desc,df("name").asc).show()
            //根据age字段降序排列，age值相同时根据name进行升序排列
```

### RDD转换得到DataFrame

反射机制推断RDD

```scala
//该方法需要定义一个case class可以将spark隐式转化为DataFrame
spark.sparkContext.textFile("people.txt").map(_.split(",")).map(attr => People(attr(0),attr(1).trim.toInt)).toDF()
        //spark.sparkContext.textFile("people.txt")执行后系统生成一个RDD
//通过toDF()操作执行后，DataFrame每条记录的模式都匹配case class


//生成DataFrame后可以进行SQL查询，但必须注册为临时表
df.createOrReplaceTempView("people")


//使用SQL语句查询
val df = spark.sql("select name,age from people where age > 20")
df.map(t => "name:"+t(0)+","+"age:"+t(1)).show()            //格式化后进行输出
```

编程方式定义RDD

无法封装为 case class 时，需要通过编程方式制作表结构

```scala
// 生成字段并封装为schema
val fields = Array(StructField("name",StringType,true),("age",IntegerType,true))
val schema = StructType(fields)
```

该模式下，需要将记录封装为Row

```scala
val rdd = rdd.map(_.split(",")).map(attr => Row(attr(0),attr(1)))
```

将表头 schema 和表记录 rdd 拼装起来

```scala
val df = spark.createDataFrame(rdd, schema)
```

### Spark SQL 读写数据库

JDBC连接数据库

```shell
spark-shell --jars mysql-connector-java-8.0.54.jar --driver-class-path mysql-connector-java-8.0.54.jar 
```

读取MySQL数据库

```scala
val df = spark.read.format("jdbc").
    option("url","jdbc:mysql://localhost:3306/spark").
    option("driver","com.mysql.cj.jdbc.Driver").
    option("dbtable","student").
    option("user","root").
    option("password","passwd").
    load()
df.show()
```

向MySQL数据库写入数据

```scala
// 准备好Row封装好的表记录RDD，并连接schema创建DataFrame
val df = spark.createDataFrame(rdd, schema)

// 使用props变量保存JDBC链接参数
val prop = new Properties()
prop.put("user","root")
prop.put("password","passwd")
prop.put("driver","com.mysql.cj.jdbc.Driver")


//使用追加方式将记录写入数据库中
df.write.mode("append").jdbc("jdbc:mysql://localhost:3306/spark","spark.student",prop)
```

# 实验结果记录

1、Spark SQL 读取 JSON文件

```shell
scala> val df = spark.read.json("employee.json")
df: org.apache.spark.sql.DataFrame = [age: bigint, id: bigint ... 1 more field] 

scala> df.show()
+----+---+-----+
| age| id| name|
+----+---+-----+
|  36|  1| Ella|
|  29|  2|  Bob|
|  29|  3| Jack|
|  28|  4|  Jim|
|  28|  4|  Jim|
|null|  5|Damon|
|null|  5|Damon|
+----+---+-----+


scala> df.distinct().show()
+----+---+-----+
| age| id| name|
+----+---+-----+
|  29|  3| Jack|
|null|  5|Damon|
|  29|  2|  Bob|
|  36|  1| Ella|
|  28|  4|  Jim|
+----+---+-----+


scala> df.select(df("age"),df("name")).show()
+----+-----+
| age| name|
+----+-----+
|  36| Ella|
|  29|  Bob|
|  29| Jack|
|  28|  Jim|
|  28|  Jim|
|null|Damon|
|null|Damon|
+----+-----+


scala> df.filter(df("age")>30).show()
+---+---+----+
|age| id|name|
+---+---+----+
| 36|  1|Ella|
+---+---+----+


scala> df.groupBy("age").count().show()
+----+-----+
| age|count|
+----+-----+
|  29|    2|
|null|    2|
|  28|    2|
|  36|    1|
+----+-----+


scala> df.sort(df("name").asc).show()
+----+---+-----+
| age| id| name|
+----+---+-----+
|  29|  2|  Bob|
|null|  5|Damon|
|null|  5|Damon|
|  36|  1| Ella|
|  29|  3| Jack|
|  28|  4|  Jim|
|  28|  4|  Jim|
+----+---+-----+


scala> df.limit(3).show()
+---+---+----+
|age| id|name|
+---+---+----+
| 36|  1|Ella|
| 29|  2| Bob|
| 29|  3|Jack|
+---+---+----+


scala> df.select(df("name").as("username")).show()
+--------+
|username|
+--------+
|    Ella|
|     Bob|
|    Jack|
|     Jim|
|     Jim|
|   Damon|
|   Damon|
+--------+


scala> df.select(avg("age")).show()
+--------+
|avg(age)|
+--------+
|    30.0|
+--------+


scala> df.select(min("age")).show()
+--------+
|min(age)|
+--------+
|      28|
+--------+
```

2、实现RDD转化为DataFrame

```scala
package ac.sict.reid.leo.SQL

import org.apache.spark.sql.SparkSession

object EmployeeSQL {

  case class Employee(id:String,name:String,age:Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("EmployeeSQL").getOrCreate()
    import spark.implicits._
        //实现DataFrame隐式转换
    val rdd = spark.sparkContext.textFile("data/employee.txt").
                      map(_.split(",")).
                      map(attr => Employee(attr(0), attr(1), attr(2).toInt))
    val df = rdd.toDF()
    val value = df.map(t =>
      "id:" + t(0) + ",name:" + t(1) + ",age:" + t(2)
    ).show()
  }
}
```

3、Spark SQL连接数据库

实验使用PostgreSQL作为关系数据库

前提：本次实验中Spark安装在Docker容器中，而PostgreSQL数据库在宿主机内。

```scala
$ spark-shell --jars postgresql-42.6.0.jar --driver-class-path postgresql-42.6.0.jar


scala> val df = spark.read.format("jdbc").
                    option("url","jdbc:postgresql://host.docker.internal:5432/sparktest").
                    option("dbtable","employee").
                    option("user","postgres").
                    option("password","wangyanlei306").load()
df: org.apache.spark.sql.DataFrame = [id: string, name: string ... 2 more fields]


scala> df.show()
+---+-----+------+---+                                                          
| id| name|gender|age|
+---+-----+------+---+
|  1|Alice|     F| 22|
|  2| John|     M| 25|
+---+-----+------+---+

scala> import spark.implicits._
import spark.implicits._

scala> val person1 = Person("3","Mary","F",26)
person1: Person = Person(3,Mary,F,26)

scala> val person2 = Person("4","Tom","M",23)
person2: Person = Person(4,Tom,M,23)

scala> val rdd = spark.sparkContext.makeRDD(Seq(person1,person2))
rdd: org.apache.spark.rdd.RDD[Person] = ParallelCollectionRDD[6] at makeRDD at <console>:28

scala> rdd.collect.foreach(println)
Person(3,Mary,F,26)
Person(4,Tom,M,23)

scala> val df = rdd.toDF()
df: org.apache.spark.sql.DataFrame = [id: string, name: string ... 2 more fields]

scala> df.show()
+---+----+------+---+
| id|name|gender|age|
+---+----+------+---+
|  3|Mary|     F| 26|
|  4| Tom|     M| 23|
+---+----+------+---+

scala> import java.util.Properties
import java.util.Properties

scala> val prop = new Properties()
prop: java.util.Properties = {}

scala> prop.put("user","postgres")
res7: Object = null

scala> prop.put("password","wangyanlei306")
res8: Object = null

scala> df.write.mode("append").jdbc("jdbc:postgresql://host.docker.internal:5432/sparktest","employee",prop)

sparktest=# select * from employee;
 id | name  | gender | age 
----+-------+--------+-----
 1  | Alice | F      |  22
 2  | John  | M      |  25
 4  | Tom   | M      |  23
 3  | Mary  | F      |  26
(4 rows)


scala> df.select(avg("age")+max("age")).show()
+---------------------+
|(avg(age) + max(age))|
+---------------------+
|                 50.0|
+---------------------+
```

（2023年8月20日于SICT-REID）
