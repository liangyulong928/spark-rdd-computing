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
