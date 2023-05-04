package udf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.{concat, lit}

object nametoU {
  
def toUpper =(FirstName:String)=> {
  val Fname = FirstName.split(" ")
  Fname.map(f => f.substring(0,1).toUpperCase()+f.substring(1,f.length())).mkString("_")
 
}
  
  def main(args: Array[String]) {
     val spark:SparkSession =SparkSession.builder().master("local").appName("Create UDF").getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
     import spark.implicits._

      val emp = Seq((1,"ramu","A",25000),(2,"raju","T",40000),
                 (3,"mahesh","B",35000), (4,"likitha","S",35000), (5,"lavanya","P",10000 ))
      
      val  df = emp toDF("Id","FirstName","LastName","Salary") 
     
     //  df.map(x => x.getString(1).trim().split(' ').map(_.capitalize).mkString(" ")).show()
      
      val UpperUDF = udf(toUpper)
        
      val UName =df.select(col("Id"),UpperUDF(col("FirstName")) as "Name",col("LastName"),col("Salary"))
               .withColumn("FullName",concat(col("Name"),lit(" "),col("LastName"))).show(false)
          
          
    //spark.sql
         spark.udf.register("sqlUDF",toUpper)
          df.createOrReplaceTempView("Name_table")
          spark.sql("select Id,FirstName,LastName,Salary, CONCAT(sqlUDF(FirstName),' ',LastName) as FullName from Name_table")
       .show(false)
          
          
}

}
