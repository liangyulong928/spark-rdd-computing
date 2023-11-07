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

    val spark = SparkSession.builder().master("local").appName("logistic").getOrCreate()
    import spark.implicits._
    val data = spark.sparkContext.textFile("data/iris.data")
      .map(_.split(",")).map(
      p => Iris(Vectors.dense(p(0).toFloat, p(1).toFloat, p(2).toFloat, p(3).toFloat), p(4).toString)
    ).toDF()
    print("原始数据结果展示:\n")
    data.show()

    print("--------------------------------------------\n")
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(data)
    val lr = new LogisticRegression().setLabelCol("indexedLabel").
                        setFeaturesCol("indexedFeatures").
                        setMaxIter(100).
                        setRegParam(0.3).
                        setElasticNetParam(0.8)
    println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    val lrPipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, lr, labelConverter))
    val Array(trainingData,testData) = data.randomSplit(Array(0.7, 0.3))
    print("--------------------------------------------\n开始训练数据\n")
    val lrPipelineModel = lrPipeline.fit(trainingData)
    val lrPredictions = lrPipelineModel.transform(testData)
    print("--------------------------------------------\n预测结果：\n")
    lrPredictions.select("predictedLabel","label","features","probability").collect().foreach {
      case Row(predictedLabel: String, label: String, features: Vector, prob: Vector) =>
        println(s"($label, $features) --> prob=$prob, predicted Label = $predictedLabel")
    }
    print("--------------------------------------------\n模型评估：\n")
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
    val lrAccuracy = evaluator.evaluate(lrPredictions)
    println(lrAccuracy)
  }

}
