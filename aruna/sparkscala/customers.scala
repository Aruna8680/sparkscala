



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object customer {
  def main(args:Array[String]){
   val  Spark: SparkSession=SparkSession.builder().master("local").appName("customer").getOrCreate()
   Spark.sparkContext.setLogLevel("ERROR")
  import Spark.implicits._
  
  val customerdf = Spark.read.format("csv").option("header", true).option("Inferschema",true)
                        .csv("C:\\Users\\paramesh\\OneDrive\\Desktop\\newcsv\\customer.csv")
                        .toDF("Customerid","Name","Gender","Age","Orders")
                        
       //customerdf.show(false) 
       //customerdf.printSchema()
   
 
       
 ////1.count the total number of customers ////  
      
 println(customerdf.columns.mkString(", "))
 val count =customerdf.select("customerid").distinct().count()
 println("Count: " + count)
 val count1=customerdf.agg(countDistinct("customerid").as("num_cus")).collect()(0)(0)
 println(s"total number of customers: $count1")
 
 
 //2.find the average age of all customers//
 customerdf.filter(col("Age").isNull).count()
 println("Count: " + count)
 val avg_age=customerdf.agg(avg("Age")).show()
 val avgage=customerdf.agg(avg(col("Age")).as("Avg_age")).collect()(0)(0).asInstanceOf[Double]
   println(s"average age of all customers $avgage")
 val avgAge = customerdf.selectExpr("avg(Age) as AverageAge").first().getDouble(0)
println(s"The average age of all customers is $avgAge")
 


//3 find the top 10 customers with the highest purchase frequency assume that purchase frequency is calculated as the number
//of orders placed by a customer over a given time period.

val purchaseFre = customerdf.groupBy("customerid").agg(countDistinct("Orders").as("purchaseFrequency"))
val topcustomers =purchaseFre.orderBy(col("purchaseFrequency").desc).limit(10)
val top10 = topcustomers.join(customerdf,"customerid").select("*").show()
 
val top10customers = customerdf.groupBy("CustomerID")
  .agg(countDistinct("Orders").as("PurchaseFrequency"))
  .orderBy(col("PurchaseFrequency").desc)
  .limit(10).join(customerdf,"customerid").select("*").show()

       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
       
  }
}