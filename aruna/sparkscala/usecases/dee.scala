package usecases

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object UnemploymentRateByStore {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Unemployment Rate By Store")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val walmartSchema = StructType(Array(
      StructField("Store", IntegerType),
      StructField("Date", DateType),
      StructField("IsHoliday", BooleanType),
      StructField("Dept", IntegerType),
      StructField("Weekly_Sales", DoubleType),
      StructField("Unemployment", DoubleType)
    ))

    val walmartDF = spark.read.format("csv")
      .option("header", "true")
      .schema(walmartSchema)
      .option("nullValue", "NA")
      .option("dateFormat", "MM/dd/yy")
      .load("C:\\Users\\paramesh\\OneDrive\\Desktop\\csvfiles\\walmart.csv").toDF()
      //walmartDF.printSchema()
      //walmartDF.show()
     
      val dateStoreDF = walmartDF.select(date_format($"Date", "yyyy-MM-dd").alias("Date"), $"Store".alias("Store") )
      dateStoreDF.show()
      val unemploymentDF = walmartDF.select($"Store", $"Date", $"Unemployment")
      .groupBy($"Store", $"Date")
      .agg(avg($"Unemployment").alias("AvgUnemploymentRate"))
      .orderBy($"Store", $"Date")

       val resultDF = dateStoreDF.join( unemploymentDF, Seq("Date", "Store"), "left")

       resultDF.show()
    
       
       
       
       //my version
       val datewise =walmartDF.groupBy("Store", "Date").agg(avg("Unemployment").as("Umemployment_rate"))
                                                         .withColumnRenamed("Store", "Store id")
       
       val unemployerate = datewise.select("Store id","Date","Umemployment_rate")
    
   
    
  }
}