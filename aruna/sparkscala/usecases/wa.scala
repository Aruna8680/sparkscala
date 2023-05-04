package usecases
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{IntegerType,StringType,DoubleType,DateType,BooleanType}
import org.apache.spark.sql.types.{StructType,StructField}

import org.apache.spark.sql.{SQLContext,SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._

object maxweeklysales {
  
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("MaxWeeklySales").master("local[*]").getOrCreate()
            
      import spark.implicits._

      spark.sparkContext.setLogLevel("ERROR")
      val newSchema = StructType(Array(
  StructField("Store", IntegerType, true),
  StructField("Date", DateType, true),
  StructField("IsHoliday", BooleanType, true),
  StructField("Dept", IntegerType, true),
  StructField("Weekly_Sales", DoubleType, true),
  StructField("Unemployment", DoubleType, true),
  StructField("_c6", StringType, true),
  StructField("_c7", StringType, true),
  StructField("_c8", DoubleType, true)
))

      val newdata = spark.read .option("header", true) .option("nullValue", "NA").option("dateFormat", "MM/dd/yyyy").schema(newSchema)
                                  .csv("C:\\Users\\paramesh\\OneDrive\\Desktop\\csvfiles\\walmart.csv").toDF()
      //newdata.printSchema()
      newdata.show()


//Find the store which has the maximum weekly sales. (Store id, maximum weekly sales)
    val maxsales =newdata.groupBy(col("Store").as("Store id")).agg(sum("Weekly_Sales").alias("maximum weekly sales"))
                                          .orderBy(desc("maximum weekly sales")).limit(1)
     //maxsales.show()
                                
////*Find out the total weekly sales value on a particular day for all the stores (Store id, date, total_weekly_sales)*???

      val totalsales1 =newdata.where("Date = '2010-02-05'").groupBy("Store","Date")
                           .agg(sum("Weekly_Sales").alias("Total_Weekly_Sales"))
                                       
       //totalsales1.show()

////**Weekly sales per department for a store (Dept, Weely Sales)**////
      
// Add a new column for the weekday
   val walmartweekly = newdata.withColumn("Weekday", weekofyear(col("Date")))

// Group the data by department and week number, and aggregate the sales
   val weekperdept =  walmartweekly.groupBy("Dept", "Weekday")
                                   .agg(sum("Weekly_Sales").as("WeeklySales"))

// Select the department and weekly sales columns and show the resulting DataFrame
    val weeksale_dept = weekperdept.select("Dept", "WeeklySales", "Weekday")
     //weeksale_dept.show()
 
 
 ////*Which store has the maximum standard deviation(the sales vary a lot) (Store id, standard deviation)*//// 
    val maxstddv = newdata.groupBy(col("Store").as("Store id")).agg(stddev_pop("Weekly_Sales").as("standard deviation")).
                       orderBy(col("standard deviation").desc).limit(1)
                       .select("Store id", "standard deviation")
      // maxstddv.show()
    
////*Which store has the monthly growth rate(percentage) (Store id, Month, growth rate)*/////
     
 // Add a new column for the month of each sale              
       val monthlydf = newdata.withColumn("month",month(col("Date")))
                                                                 
       
 // Group the data by store and month, and aggregate the sales
       val monthly_sales = monthlydf.groupBy("Store", "month").agg(sum("Weekly_Sales").as("totalSales"))

// Add a new column for the sales in the previous month
       val winspec = Window.partitionBy("Store").orderBy("month")
 
       val previos_M_sales = monthly_sales.withColumn("premontlysales", lag("totalSales",1).over(winspec))
 // Calculate the monthly growth rate as a percentage
       val growth_rate = previos_M_sales.withColumn("growthrate" ,when(col("premontlysales") isNull,null)
                                 .otherwise(((col("totalSales")-col("premontlysales"))/col("premontlysales"))*100))
                          
       val result =  growth_rate.select("Store","month","growthrate").orderBy(col("growthrate").desc_nulls_last).limit(1)
      // result.show()


///*Find the Date-wise unemployment rate for every store. (Store id, Date,Umemployment_rate)*///
       
       val daywise =newdata.withColumn("day",dayofmonth(col("Date")))
       val unemprate =daywise.groupBy("Store", "day").agg(avg("Unemployment").as("Umemployment_rate"))
                                               .withColumnRenamed("Store", "Store id")
       val Un_emp_rate =unemprate.select("Store id","day","Umemployment_rate").show()
       
       
       
       val datewise =newdata.groupBy("Store", "Date").agg(avg("Unemployment").as("Umemployment_rate"))
                                                         .withColumnRenamed("Store", "Store id")
       
       val unemployerate = datewise.select("Store id","Date","Umemployment_rate")
       
///*Find holidays which have higher sales than the mean sales in non-holiday date for each store(Store id, Date, Is holiday, Sales)
    
       val windowspec = Window.partitionBy("Store", "IsHoliday")
    
       val meansales1 = newdata.withColumn("meansales",avg(when($"IsHoliday" === false, $"Weekly_Sales"))
                            .over(windowspec)).na.fill(0, Seq("meansales")).orderBy($"meansales".desc)
    
       val result1=meansales1.filter($"IsHoliday" === true && $"Weekly_Sales" > $"meansales")
                           .select("Store","IsHoliday","Date","Weekly_Sales")
    
       
        
       
//* Get the 10th highest weekly sales per month and year (Store id, Year/Month, Weekly Sales)
       
 val yearMonthSales = newdata.withColumn("YearMonth", date_format($"Date", "yyyy-MM")) 
                             .groupBy("Store", "YearMonth") .agg(max("Weekly_Sales").as("MaxSales")) 
                             .withColumn("Rank", dense_rank().over(Window.partitionBy("YearMonth").orderBy($"MaxSales".desc)))

val res = yearMonthSales .filter($"Rank" === 10) .select("Store", "YearMonth", "MaxSales")

res.show()









       
       
       
       
  }
}