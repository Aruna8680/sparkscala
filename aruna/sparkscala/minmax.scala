// Imports



import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

class minmax1 {
  def main(args:Array[String]): Unit = {
// Create SparkSession
val spark = SparkSession.builder()
        .appName("Creating DataFrame")
        .master("local[*]")
        .getOrCreate()

// Create DataFrame
val customerData = List(
      ("John", "1", "20000"),
      ("Jane", "2", "30000"),
      ("Bob", "3", "40000")
    )
val rdd = spark.sparkContext.parallelize(customerData)
import spark.implicits._
val df = rdd.toDF("name", "id", "salary")

// Get min & max value of a column
val min_value = df.agg(min("salary")).head().get(0)
val max_value = df.agg(max("salary")).head().get(0)
println(max_value)

}
}