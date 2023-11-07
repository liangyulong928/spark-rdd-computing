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
