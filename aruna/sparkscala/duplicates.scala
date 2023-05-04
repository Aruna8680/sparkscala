
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object duplicates1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Duplicates").master("local").getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
 var readcsv = spark.read.option("header","true").
                        csv("C:\\Users\\paramesh\\OneDrive\\Desktop\\csv files\\duplicates.csv")
                        
readcsv.show()
  var windowDF = Window.partitionBy("CustomerId").orderBy("OrderAmount")
    
var DF = readcsv.withColumn("dense_rank",dense_rank().over(windowDF))
  
var df1 = DF.where(col("dense_rank")===1).dropDuplicates()
     
   df1.show()
    
  // df1.coalesce(1).write.mode("Append").format("csv").csv("C:\\Users\\paramesh\\OneDrive\\Desktop\\result")
 

 
   
}        

}