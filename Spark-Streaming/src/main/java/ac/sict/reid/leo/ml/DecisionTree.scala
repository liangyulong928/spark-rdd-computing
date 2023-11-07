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
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)
    val vectorIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(data)
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictionLabel").setLabels(labelIndexer.labels)
    val treeClassifier = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")
    val pipeline = new Pipeline().setStages(Array(labelIndexer, vectorIndexer, treeClassifier, labelConverter))

    print("--------------------------------------------\n模型训练:\n")
    //TODO 训练
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    val model = pipeline.fit(trainingData)
    val predictionData = model.transform(testData)
    predictionData.select("predictionLabel","label","features").show()


    print("--------------------------------------------\n")
    //TODO 模型评估
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("prediction").setLabelCol("indexedLabel")
    val accuracy = evaluator.evaluate(predictionData)
    print("模型评估结果：" + accuracy)

  }
}
