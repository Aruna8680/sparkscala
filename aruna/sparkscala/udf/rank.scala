


  import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object rank6 {
    def main(args: Array[String]) {

  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("rank")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val simpleData = Seq(("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  )
  var  df = simpleData.toDF("employee_name", "department", "salary")
  
  df.show()
  var windowSpec  = Window.partitionBy("department").orderBy("salary")
  var df1 = df.withColumn("rank",dense_rank().over(windowSpec))
  df1.show()
  var df2 = df1. where($"rank"=== 2).show()
  
  
}
  }