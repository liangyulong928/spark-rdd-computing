package ac.sict.reid.leo.SQL

import org.apache.spark.sql.SparkSession

object EmployeeSQL {

  case class Employee(id:String,name:String,age:Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("EmployeeSQL").getOrCreate()
    import spark.implicits._
    val rdd = spark.sparkContext.textFile("data/employee.txt").
                      map(_.split(",")).
                      map(attr => Employee(attr(0), attr(1), attr(2).toInt))
    val df = rdd.toDF()
    val value = df.map(t =>
      "id:" + t(0) + ",name:" + t(1) + ",age:" + t(2)
    ).show()
  }
}
