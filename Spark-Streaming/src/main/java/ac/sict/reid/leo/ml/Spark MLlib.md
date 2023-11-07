# Spark MLlib

MLlib 由一些通用的学习算法和工具组成，包括分类、回归、聚类、协同过滤、降维等。

#### 基本数据类型

稠密向量和稀疏向量

```scala
scala> import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.{Vector, Vectors}

// 创建一个稠密本地向量
scala> val dv = Vectors.dense(2.0,0.0,8.0)
dv: org.apache.spark.ml.linalg.Vector = [2.0,0.0,8.0]

// 创建一个稀疏本地向量
// 参数：（向量长度，非零元素索引，非零元素值）
scala> val svl = Vectors.sparse(3,Array(0,2),Array(2,8.0))
svl: org.apache.spark.ml.linalg.Vector = (3,[0,2],[2.0,8.0])
// 参数：（向量长度，Seq((索引，值)）
scala> val sv2 = Vectors.sparse(3,Seq((0,2.0),(2,8.0)))
sv2: org.apache.spark.ml.linalg.Vector = (3,[0,2],[2.0,8.0])
```

标注点：带有标签的本地向量

```scala
scala> import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vectors

scala>  import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.feature.LabeledPoint

scala> val pos = LabeledPoint(1.0,Vectors.dense(2.0,0.0,8.0))
pos: org.apache.spark.ml.feature.LabeledPoint = (1.0,[2.0,0.0,8.0])

scala> val neg = LabeledPoint(0.0,Vectors.sparse(3,Array(0,2),Array(2.0,8.0)))
neg: org.apache.spark.ml.feature.LabeledPoint = (0.0,(3,[0,2],[2.0,8.0]))
```

稠密矩阵与稀疏矩阵

```scala
scala> import org.apache.spark.ml.linalg.{Matrix,Matrices}
import org.apache.spark.ml.linalg.{Matrix, Matrices}

scala> val dm = Matrices.dense(3,2,Array(1.0,3.0,5.0,2.0,4.0,6.0))
dm: org.apache.spark.ml.linalg.Matrix =
1.0  2.0
3.0  4.0
5.0  6.0

scala> val sm = Matrices.sparse(3,2,Array(0,1,3),Array(0,2,1),Array(9,6,8))
sm: org.apache.spark.ml.linalg.Matrix =
3 x 2 CSCMatrix
(0,0) 9.0
(2,1) 6.0
(1,1) 8.0
```

#### 机器学习流水线工作过程

PipelineStage 称为工作流阶段，包括转换器和评估器。对特定问题转换器和评估器，可以按照具体处理逻辑组织PipelineStage创建流水线。

```scala
val pipeline = new Pipeline().setStages(Array(stage1,stage2,stage3,...))
```

在流水线中，上一个 PipelineStage 的输出恰好是下一个 PipelineStage 的输入。调用 fit() 方法以流的方式训练源数据。

#### 特征提取

指利用已有特征计算出一个抽象程度更高的特征集

示例代码展示TF-IDF特征提取过程

```scala
scala> import org.apache.spark.ml.feature.{HashingTF,IDF,Tokenizer}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
                                                                                                   scala> val sentenceData = spark.createDataFrame(Seq((0,"I heard about Spark and I love Spark"),(0,"I wish Java could use case classes"),(1,"Logistic regression models are neat"))).toDF("label","sent")
sentenceData: org.apache.spark.sql.DataFrame = [label: int, sentence: string]

scala> val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
tokenizer: org.apache.spark.ml.feature.Tokenizer = tok_1ab95f066343

scala> val wordsData = tokenizer.transform(sentenceData)
wordsData: org.apache.spark.sql.DataFrame = [label: int, sentence: string ... 1 more field]

scala> wordsData.show(false)
+-----+------------------------------------+---------------------------------------------+
|label|sentence                            |words                                        |
+-----+------------------------------------+---------------------------------------------+
|0    |I heard about Spark and I love Spark|[i, heard, about, spark, and, i, love, spark]|
|0    |I wish Java could use case classes  |[i, wish, java, could, use, case, classes]   |
|1    |Logistic regression models are neat |[logistic, regression, models, are, neat]    |
+-----+------------------------------------+---------------------------------------------+

scala> val hashingTF = new HashingTF().setInputCol("words").
																setOutputCol("rawFeatures").
																setNumFeatures(2000)
hashingTF: org.apache.spark.ml.feature.HashingTF = HashingTF: uid=hashingTF_dcddae8df8e5, binary=false, numFeatures=2000

scala> val featurizedData = hashingTF.transform(wordsData)
featurizedData: org.apache.spark.sql.DataFrame = [label: int, sentence: string ... 2 more fields]

scala> featurizedData.select("words","rawFeatures").show(false)
+---------------------------------------------+--------------------------------------------+
|words                                        |rawFeatures                                 |
+---------------------------------------------+--------------------------------------------+
|[i, heard, about, spark, and, i, love, spark]|(2000,[240,...,1756],[1.0,...,2.0])         |
|[i, wish, java, could, use, case, classes]   |(2000,[80,...,1967],[1.0,...,1.0])          |
|[logistic, regression, models, are, neat]    |(2000,[286,...,1871],[1.0,...,1.0])         |
+---------------------------------------------+--------------------------------------------+
scala> val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
idf: org.apache.spark.ml.feature.IDF = idf_8133a2bcb73e

scala> val idfModel = idf.fit(featurizedData)
idfModel: org.apache.spark.ml.feature.IDFModel = IDFModel: uid=idf_8133a2bcb73e, numDocs=3, numFeatures=2000

scala> val rescaledData = idfModel.transform(featurizedData)
rescaledData: org.apache.spark.sql.DataFrame = [label: int, sentence: string ... 3 more fields]

scala> rescaledData.select("features","label").show(false)
+----------------------------------------------------------------------------------+-----+
|                               features                                           |label|                                                                                                                                                                  
+----------------------------------------------------------------------------------+-----+
|(2000,[240,...,1756],[0.6931471805599453,...,0.5753641449035617])                 |0    |
|(2000,[80,...,1967],[0.6931471805599453,...,0.6931471805599453])                  |0    |
|(2000,[286,...,1871],[0.6931471805599453,...,0.6931471805599453])                 |1    |
+----------------------------------------------------------------------------------+-----+
```

#### 特征转换

```scala
scala> val df1 = spark.createDataFrame(Seq((0,"a"),(1,"b"),(2,"c"),(3,"a"),(4,"a"),(5,"c"))).toDF("d","category")
df1: org.apache.spark.sql.DataFrame = [id: int, category: string]

//1.StringIndexer:标签按频率由大到小排序
scala> val indexer = new StringIndexer().
													setInputCol("category").setOutputCol("categoryIndex")
indexer: org.apache.spark.ml.feature.StringIndexer = strIdx_72ea498afd7f

scala> val indexed1 = indexer.fit(df1).transform(df1)
indexed1: org.apache.spark.sql.DataFrame = [id: int, category: string ... 1 more field]

scala> indexed1.show()
+---+--------+-------------+
| id|category|categoryIndex|
+---+--------+-------------+
|  0|       a|          0.0|
|  1|       b|          2.0|
|  2|       c|          1.0|
|  3|       a|          0.0|
|  4|       a|          0.0|
|  5|       c|          1.0|
+---+--------+-------------+

//2.IndexToString: 把标签索引的一列重新映射到原有的字符型标签
scala> import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.feature.IndexToString
                                                                                                   scala> val tostring = new IndexToString().
													setInputCol("categoryIndex").setOutputCol("originalCategory")
tostring: org.apache.spark.ml.feature.IndexToString = idxToStr_351b80092567

scala> val indexString = tostring.transform(indexed1)
indexString: org.apache.spark.sql.DataFrame = [id: int, category: string ... 2 more fields]

scala> indexString.select("id","originalCategory").show()
+---+----------------+
| id|originalCategory|
+---+----------------+
|  0|               a|
|  1|               b|
|  2|               c|
|  3|               a|
|  4|               a|
|  5|               c|
+---+----------------+

//3.VectorIndexer
scala> val data = Seq(Vectors.dense(-1.0,1.0,1.0),
                  Vectors.dense(-1.0,3.0,1.0),
                  Vectors.dense(0.0,5.0,1.0))
data: Seq[org.apache.spark.ml.linalg.Vector] = List([-1.0,1.0,1.0], [-1.0,3.0,1.0], [0.0,5.0,1.0])

scala> val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
df: org.apache.spark.sql.DataFrame = [features: vector]

scala> val indexer = new VectorIndexer().
     | 								setInputCol("features").
     | 								setOutputCol("indexed").
     | 								setMaxCategories(2)
indexer: org.apache.spark.ml.feature.VectorIndexer = vecIdx_8edbc07a5050

scala> val indexerModel = indexer.fit(df)
indexerModel: org.apache.spark.ml.feature.VectorIndexerModel = VectorIndexerModel: uid=vecIdx_8edbc07a5050, numFeatures=3, handleInvalid=error

scala> val cateFeatures = indexerModel.categoryMaps.keys.toSet
cateFeatures: scala.collection.immutable.Set[Int] = Set(0, 2)

scala> println(s"Chose ${cateFeatures.size} categorical fearures: "+ cateFeatures.mkString(","))
Chose 2 categorical fearures: 0,2

scala> val indexed = indexerModel.transform(df)
indexed: org.apache.spark.sql.DataFrame = [features: vector, indexed: vector]

scala> indexed.show()
+--------------+-------------+
|      features|      indexed|
+--------------+-------------+
|[-1.0,1.0,1.0]|[1.0,1.0,0.0]|
|[-1.0,3.0,1.0]|[1.0,3.0,0.0]|
| [0.0,5.0,1.0]|[0.0,5.0,0.0]|
+--------------+-------------+
```

#### 特征选择

```scala
scala> val df = spark.createDataFrame(Seq((1,Vectors.dense(0.0,0.0,18.0,1.0),1),
     | (2,Vectors.dense(0.0,1.0,12.0,0.0),0),
     | (3,Vectors.dense(1.0,0.0,15.0,0.1),0))).toDF("id","features","label")
df: org.apache.spark.sql.DataFrame = [id: int, features: vector ... 1 more field]

scala> df.show()
+---+------------------+-----+
| id|          features|label|
+---+------------------+-----+
|  1|[0.0,0.0,18.0,1.0]|    1|
|  2|[0.0,1.0,12.0,0.0]|    0|
|  3|[1.0,0.0,15.0,0.1]|    0|
+---+------------------+-----+

scala> import org.apache.spark.ml.feature.{ChiSqSelector,ChiSqSelectorModel}
import org.apache.spark.ml.feature.{ChiSqSelector, ChiSqSelectorModel}

scala> val selector = new ChiSqSelector().
     | setNumTopFeatures(1).
     | setFeaturesCol("features").
     | setLabelCol("label").
     | setOutputCol("selected-feature")
selector: org.apache.spark.ml.feature.ChiSqSelector = chiSqSelector_f224fa097ab2

scala> val selector_model = selector.fit(df)
selector_model: org.apache.spark.ml.feature.ChiSqSelectorModel = ChiSqSelectorModel: uid=chiSqSelector_f224fa097ab2, numSelectedFeatures=1

scala> val result = selector_model.transform(df)
result: org.apache.spark.sql.DataFrame = [id: int, features: vector ... 2 more fields]

scala> result.show(false)
+---+------------------+-----+----------------+
|id |features          |label|selected-feature|
+---+------------------+-----+----------------+
|1  |[0.0,0.0,18.0,1.0]|1    |[18.0]          |
|2  |[0.0,1.0,12.0,0.0]|0    |[12.0]          |
|3  |[1.0,0.0,15.0,0.1]|0    |[15.0]          |
+---+------------------+-----+----------------+
```

### 分类算法

1、Logistic回归

```scala
package ac.sict.reid.leo.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}

object Logistic {

  case class Iris(features: org.apache.spark.ml.linalg.Vector,label:String)

  def main(args: Array[String]): Unit = {

    //TODO 对原始的数据将其封装为DataFrame
    val spark = SparkSession.builder().master("local").appName("logistic").getOrCreate()
    import spark.implicits._
    val data = spark.sparkContext.textFile("data/iris.data")
      .map(_.split(",")).map(
      p => Iris(Vectors.dense(p(0).toFloat, p(1).toFloat, p(2).toFloat, p(3).toFloat), p(4).toString)
    ).toDF()
    print("原始数据结果展示:\n")
    data.show()

    //TODO 构建LR模型
    print("--------------------------------------------\n")
    val labelIndexer = new StringIndexer().setInputCol("label").
    													setOutputCol("indexedLabel").fit(data)
    val featureIndexer = new VectorIndexer().setInputCol("features").
    													setOutputCol("indexedFeatures").fit(data)
    val lr = new LogisticRegression().setLabelCol("indexedLabel").
                        setFeaturesCol("indexedFeatures").
                        setMaxIter(100).					//循环次数：100次
                        setRegParam(0.3).					//规范化项：0.3
                        setElasticNetParam(0.8)		//正则项系数
    println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

    val labelConverter = new IndexToString().setInputCol("prediction").
    								setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    val lrPipeline = new Pipeline().
    								setStages(Array(labelIndexer, featureIndexer, lr, labelConverter))
    
    //TODO 划分训练集和测试集
    val Array(trainingData,testData) = data.randomSplit(Array(0.7, 0.3))
    print("--------------------------------------------\n开始训练数据\n")
    val lrPipelineModel = lrPipeline.fit(trainingData)
    val lrPredictions = lrPipelineModel.transform(testData)
    print("--------------------------------------------\n预测结果：\n")
    lrPredictions.select("predictedLabel","label","features","probability").
    					collect().foreach {
      case Row(predictedLabel: String, label: 
               String, features: Vector, prob: Vector) =>
        println(s"($label, $features) --> 
        					prob=$prob, predicted Label = $predictedLabel")
    }
    
    //TODO 模型评估
    print("--------------------------------------------\n模型评估：\n")
    val evaluator = new MulticlassClassificationEvaluator().
    						setLabelCol("indexedLabel").setPredictionCol("prediction")
    val lrAccuracy = evaluator.evaluate(lrPredictions)
    println(lrAccuracy)
  }
}
```

运行结果

```shell
原始数据结果展示:
+--------------------+-----------+
|            features|      label|
+--------------------+-----------+
|[5.09999990463256...|Iris-setosa|
...
|[5.09999990463256...|Iris-setosa|
+--------------------+-----------+
only showing top 20 rows

--------------------------------------------
LogisticRegression parameters:
aggregationDepth: suggested depth for treeAggregate (>= 2) (default: 2)
...

--------------------------------------------
开始训练数据
--------------------------------------------
预测结果：
(Iris-setosa, [4.300000190734863,3.0,1.100000023841858,0.10000000149011612]) --> prob=[0.5838550112961782,0.23381020228934427,0.1823347864144775], predicted Label = Iris-setosa
...
(Iris-virginica, [7.699999809265137,3.799999952316284,6.699999809265137,2.200000047683716]) --> prob=[0.11720205493498893,0.3684655149105844,0.5143324301544265], predicted Label = Iris-virginica
--------------------------------------------
模型评估：
0.5170097445655307
```

2、决策树分类器

```scala
package ac.sict.reid.leo.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object DecisionTree {

  case class Iris(features: org.apache.spark.ml.linalg.Vector,label:String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("logistic").getOrCreate()
    import spark.implicits._
    val data = spark.sparkContext.textFile("data/iris.data")
      .map(_.split(",")).map(
      p => Iris(Vectors.dense(p(0).toFloat, p(1).toFloat, p(2).toFloat, p(3).toFloat), p(4).toString)
    ).toDF()
    print("原始数据结果展示:\n")
    data.show()

    //TODO 构造训练器
    val labelIndexer = new StringIndexer().setInputCol("label").
    						setOutputCol("indexedLabel").fit(data)
    val vectorIndexer = new VectorIndexer().setInputCol("features").
    						setOutputCol("indexedFeatures").fit(data)
    val labelConverter = new IndexToString().setInputCol("prediction").
    						setOutputCol("predictionLabel").setLabels(labelIndexer.labels)
    val treeClassifier = new DecisionTreeClassifier().
    						setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")
    val pipeline = new Pipeline().setStages(Array(labelIndexer, 
                                                  vectorIndexer, 
                                                  treeClassifier, 
                                                  labelConverter))

    print("--------------------------------------------\n模型训练:\n")
    //TODO 训练
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    val model = pipeline.fit(trainingData)
    val predictionData = model.transform(testData)
    predictionData.select("predictionLabel","label","features").show()


    print("--------------------------------------------\n")
    //TODO 模型评估
    val evaluator = new MulticlassClassificationEvaluator().
    												setLabelCol("prediction").setLabelCol("indexedLabel")
    val accuracy = evaluator.evaluate(predictionData)
    print("模型评估结果：" + accuracy)

  }
}
```

运行结果

```shell
原始数据结果展示:
+--------------------+-----------+
|            features|      label|
+--------------------+-----------+
|[5.09999990463256...|Iris-setosa|
...
|[5.09999990463256...|Iris-setosa|
+--------------------+-----------+
only showing top 20 rows

--------------------------------------------
模型训练:
+---------------+---------------+--------------------+
|predictionLabel|          label|            features|
+---------------+---------------+--------------------+
|    Iris-setosa|    Iris-setosa|[4.40000009536743...|
|    Iris-setosa|    Iris-setosa|[4.59999990463256...|
|    Iris-setosa|    Iris-setosa|[4.59999990463256...|
...
|Iris-versicolor| Iris-virginica|[5.59999990463256...|
|Iris-versicolor|Iris-versicolor|[5.69999980926513...|
|Iris-versicolor|Iris-versicolor|[5.69999980926513...|
|Iris-versicolor|Iris-versicolor|[5.69999980926513...|
+---------------+---------------+--------------------+
only showing top 20 rows

--------------------------------------------
模型评估结果：0.8788521482052765
```

3、K-Means聚类

```scala
package ac.sict.reid.leo.ml

import ac.sict.reid.leo.ml.DecisionTree.Iris
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object kmeans {

  case class Iris(features: org.apache.spark.ml.linalg.Vector,label:String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("logistic").getOrCreate()
    import spark.implicits._
    val data = spark.sparkContext.textFile("data/iris.data")
      .map(_.split(",")).map(
      p => Iris(Vectors.dense(p(0).toFloat, p(1).toFloat, p(2).toFloat, p(3).toFloat), p(4).toString)
    ).toDF()
    print("原始数据结果展示:\n")
    data.show()

    print("--------------------------------------------\n构造模型\n")

    val kMeansModel = new KMeans().setK(3).setFeaturesCol("features").setPredictionCol("prediction").fit(data)

    val result = kMeansModel.transform(data)

    print("--------------------------------------------\n模型训练结果\n")
    result.collect().foreach(row=>{
      println(row(0) + " => cluster: " + row(1))
    })

    print("--------------------------------------------\n查看簇中心\n")
    kMeansModel.clusterCenters.foreach(
      center=>{
        println("Clustering Center:" + center)
      }
    )
  }
}
```

运行结果

```shell
原始数据结果展示:
+--------------------+-----------+
|            features|      label|
+--------------------+-----------+
|[5.09999990463256...|Iris-setosa|
...
|[5.09999990463256...|Iris-setosa|
+--------------------+-----------+
only showing top 20 rows

--------------------------------------------
构造模型
--------------------------------------------
模型训练结果
[5.099999904632568,3.5,1.399999976158142,0.20000000298023224] => cluster: Iris-setosa
[4.900000095367432,3.0,1.399999976158142,0.20000000298023224] => cluster: Iris-setosa
...
[5.900000095367432,3.0,5.099999904632568,1.7999999523162842] => cluster: Iris-virginica
--------------------------------------------
查看簇中心
Clustering Center:[5.9016,2.7483,4.3935,1.433]
Clustering Center:[5.0060,3.4180,1.4639,0.244]
Clustering Center:[6.8499,3.0736,5.7421,2.071]
```

4、GMM

```scala
package ac.sict.reid.leo.ml

import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object GMM {

  case class Iris(features: org.apache.spark.ml.linalg.Vector,label:String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("logistic").getOrCreate()
    import spark.implicits._
    val data = spark.sparkContext.textFile("data/iris.data")
      .map(_.split(",")).map(
      p => Iris(Vectors.dense(p(0).toFloat, p(1).toFloat, p(2).toFloat, p(3).toFloat), p(4).toString)
    ).toDF()
    print("原始数据结果展示:\n")
    data.show()

    print("--------------------------------------------\n构造模型\n")
    val gm = new GaussianMixture().setK(3).setPredictionCol("prediction").setProbabilityCol("probability")
    val model = gm.fit(data)

    val result = model.transform(data)
    result.select("label","prediction","probability").show()

  }
}
```

运行结果

```shell
原始数据结果展示:
+--------------------+-----------+
|            features|      label|
+--------------------+-----------+
|[5.09999990463256...|Iris-setosa|
...
|[5.09999990463256...|Iris-setosa|
+--------------------+-----------+
only showing top 20 rows

--------------------------------------------
构造模型
+-----------+----------+--------------------+
|      label|prediction|         probability|
+-----------+----------+--------------------+
|Iris-setosa|         2|[4.68224305612849...|
...
|Iris-setosa|         2|[5.09289394664511...|
|Iris-setosa|         2|[8.66429222550741...|
+-----------+----------+--------------------+
only showing top 20 rows
```

## 协同过滤算法

在一组兴趣相同的用户或项目进行推荐，根据相似用户的偏好信息，产生目标用户的推荐列表；或者综合相似用户对某一信息的评价，形成系统对该指定用户对此信息的喜好程度预测。

#### 原理

基于用户的协同过滤算法通过不同用户对物品的评分来评测用户之间的相似性，并基于用户之间的相似性做出推荐。基于物品的协同过滤算法通过用户对不同物品的评分来评价物品之间的相似性，并基于物品之间的相似性做出推荐。

##### ALS算法（交替最小二乘法）

基于矩阵分解。对矩阵R，ALs旨在找到两个低维矩阵P和Q来逼近R。

对逼近R，通过最小化损失函数L完成：
$$
L(P,Q)=∑_{u,i}{(r_{u,i}-p_u^Tq_i)}^2 + \lambda(|p_u|^2+|q_i|^2)
$$
对 L(P,Q) 求解思想为：固定一类参数，使其变为单类变量优化问题，利用解析方法进行优化；再反过来，固定先前优化过的参数，再优化另一组参数；此过程迭代进行，直到收敛。
$$
p_u = (Q^TQ+\lambda I)^{-1}Q^Tr_u 
$$

$$
q_i = (P^TP+\lambda I)^{-1}P^Tr_i
$$

交替优化P、Q直至均方根误差小于某一预定义阈值。

##### 代码

```scala
package ac.sict.reid.leo.ml

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

object ALS {

  case class Rating(userId:Int,movieId:Int,rating:Float)

  def parseRating(str:String):Rating={
    val fields = str.split("::")
    assert(fields.size==3)
    Rating(fields(0).toInt,fields(1).toInt,fields(2).toFloat)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("logistic").getOrCreate()
    import spark.implicits._

    // 导入数据集
    val data = spark.sparkContext.textFile("data/sample_movielens_data.txt").map(parseRating).toDF()
    data.show()

    // 构建模型
    val Array(training,test) = data.randomSplit(Array(0.8, 0.2))
                      // 显性反馈
    val alsExplicit = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").
                            setItemCol("movieId").setRatingCol("rating")
                      // 隐性反馈
    val alsIxplicit = new ALS().setMaxIter(5).setRegParam(0.01).setImplicitPrefs(true).
                            setUserCol("userId").setItemCol("movieId").setRatingCol("rating")

    print("------------------------------------\n模型训练\n")
    val modelExplicit = alsExplicit.fit(training)
    val modelIxplicit = alsIxplicit.fit(training)

    // 模型预测
    val predictionExplicit = modelExplicit.transform(test).na.drop()
    val predictionIxplicit = modelIxplicit.transform(test).na.drop()


    print("------------------------------------\n显性模型预测\n")
    predictionExplicit.show()
    print("------------------------------------\n隐性模型预测\n")
    predictionIxplicit.show()
  }

}
```

运行结果

```shell
源数据
+------+-------+------+
|userId|movieId|rating|
+------+-------+------+
|     0|      2|   3.0|
|     0|      3|   1.0|
|     0|      5|   2.0|
|     0|      9|   4.0|
|     0|     11|   1.0|
|     0|     12|   2.0|
|     0|     15|   1.0|
|     0|     17|   1.0|
|     0|     19|   1.0|
|     0|     21|   1.0|
|     0|     23|   1.0|
|     0|     26|   3.0|
|     0|     27|   1.0|
|     0|     28|   1.0|
|     0|     29|   1.0|
|     0|     30|   1.0|
|     0|     31|   1.0|
|     0|     34|   1.0|
|     0|     37|   1.0|
|     0|     41|   2.0|
+------+-------+------+
only showing top 20 rows


------------------------------------
模型训练
------------------------------------
显性模型预测
+------+-------+------+-----------+
|userId|movieId|rating| prediction|
+------+-------+------+-----------+
|    14|     31|   3.0|  2.1999722|
|    26|     85|   1.0|   4.345084|
|    16|     85|   5.0| -1.2715425|
|    20|     85|   2.0|  3.2713377|
|     8|     85|   5.0|  3.3143995|
|    23|     85|   1.0|-0.68929994|
|     7|     85|   4.0|  1.4216691|
|    12|     53|   1.0| -2.1721268|
|    28|     78|   1.0|-0.34255195|
|    17|     78|   1.0|  1.2883804|
|    11|     78|   1.0|  0.7823269|
|    28|     34|   1.0|  -1.012922|
|    16|     34|   1.0|  3.1402597|
|     4|     34|   1.0|  1.1908464|
|    26|     81|   3.0|  1.1169641|
|    27|     28|   1.0| 0.19116491|
|     6|     28|   1.0|   1.358405|
|    25|     76|   1.0| 0.98397815|
|    11|     76|   1.0|  3.4053497|
|     3|     26|   1.0|  0.5203902|
+------+-------+------+-----------+
only showing top 20 rows

------------------------------------
隐性模型预测
+------+-------+------+------------+
|userId|movieId|rating|  prediction|
+------+-------+------+------------+
|    14|     31|   3.0|  0.79802024|
|    26|     85|   1.0|  0.98835176|
|    16|     85|   5.0|   0.5506306|
|    20|     85|   2.0|  0.07750125|
|     8|     85|   5.0|  0.68542016|
|    23|     85|   1.0|  0.05137192|
|     7|     85|   4.0|    0.480247|
|    12|     53|   1.0|  0.16767296|
|    28|     78|   1.0|  0.17735796|
|    17|     78|   1.0|-0.016111419|
|    11|     78|   1.0|  0.29685006|
|    28|     34|   1.0|   0.0093874|
|    16|     34|   1.0|  0.52440804|
|     4|     34|   1.0|  0.14459659|
|    26|     81|   3.0|   0.4282986|
|    27|     28|   1.0|   0.3072154|
|     6|     28|   1.0|   0.2548714|
|    25|     76|   1.0|    1.111581|
|    11|     76|   1.0|  0.29482442|
|     3|     26|   1.0|   0.3968358|
+------+-------+------+------------+
only showing top 20 rows

```

