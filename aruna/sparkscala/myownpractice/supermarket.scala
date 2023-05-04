package myownpractice
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object supermarket {
 def main(args:Array[String]){
  val  spark:SparkSession =SparkSession.builder().master("local").appName("supermarket").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  
  val supermarketdf =spark.read.format("csv").option("header",true).option("Inferschema",true).
                                csv("C:\\Users\\paramesh\\OneDrive\\Desktop\\newcsv\\supermarket.csv").toDF()
                                

  supermarketdf.printSchema()
  supermarketdf.show()
  
  //ItemID|    ItemName|Price|Quantity|Date| Sales|
  
  //return the top 5 most expensive items
  val Expdf=  supermarketdf.orderBy(col("Price")).limit(5)
  Expdf.show()
  // find the total sales revenue for each day in the Supermarket table, given columns Date and Sales.
  val totalsales = supermarketdf.groupBy("Date").agg(sum("Sales").alias("totalrevenue")).show()
  
  
  //find the average price of all items in the Supermarket table
  val Avg_price = supermarketdf.agg(avg("Price").as("AveragePrice")).show()
  
  //find the total sales revenue for each item in the Supermarket table.
  val total_sales =supermarketdf.groupBy("ItemID").agg(sum("Price").as("Tot_Revenue")).orderBy(col("ItemID").desc)show()
  
  
  //return the names of all items with a quantity greater than 100.
  
  val names_100 = supermarketdf.filter(expr("Quantity >100")).select("ItemName","Quantity").show()
  
  //find the total number of items sold for each item in the Supermarket table.
  
  val Items_sold = supermarketdf.groupBy("ItemID","Quantity").count().agg(sum("Quantity")).show()
  val totalItemsSoldDF = supermarketdf.groupBy("ItemID")
                                    .agg(sum("Quantity").alias("TotalItemsSold"))


totalItemsSoldDF.show()
}
 
 
}