  import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
 import org.apache.spark.sql.SQLContext
 import org.apache.spark.sql.RelationalGroupedDataset
object avgsal {
def main(args: Array[String]) {
val spark: SparkSession = SparkSession.builder().master("local").appName("avg_sal").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
import spark.implicits._
val empDF = spark.read.format("csv")
              .option("header",true) .option("delimeter ", ",").option("quote","")
                .csv("C:\\Users\\paramesh\\OneDrive\\Desktop\\csv files\\employees.csv").
                toDF("empid","Name" ,"Dept","sal")
               empDF.show()
 
   val depBySal = Window.partitionBy("Dept").orderBy($"empid".desc)
    val avgSal= empDF.groupBy("Dept").agg(avg("sal").as("avg_sal"))
  
      val resultDF = empDF.join(avgSal, Seq("Dept"))
      .withColumn("diff", col("avg_sal") - col("sal"))
      .withColumn("avg_sal", col("avg_sal")). withColumn("lag",lag("sal",1).over(depBySal)).show()
      
      
      
      
                
    
}
  

}

 