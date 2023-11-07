package ac.sict.reid.leo

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object CustomerSVM {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("taobaoSVM").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val train_data = sc.textFile("data/taobao/train.csv")
    val test_data = sc.textFile("data/taobao/test.csv")

    val train = train_data.map { line =>
      val parts = line.split(",")
      LabeledPoint(parts(4).toDouble, Vectors.dense(parts(1).toDouble, parts(2).toDouble, parts(3).toDouble))
    }

    val test = test_data.map { line =>
      val parts = line.split(",")
      LabeledPoint(parts(4).toDouble, Vectors.dense(parts(1).toDouble, parts(2).toDouble, parts(3).toDouble))
    }

    val numIterations = 1000
    val model = SVMWithSGD.train(train, numIterations)

    model.clearThreshold()
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      score + " " + point.label
    }
    scoreAndLabels.foreach(println)
  }

}
