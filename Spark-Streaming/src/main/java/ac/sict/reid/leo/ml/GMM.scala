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
