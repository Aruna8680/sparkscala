
 import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
 import org.apache.spark.sql.SQLContext
object EmpMgr {
   def main(args: Array[String]) {
     
     
     val spark:SparkSession =SparkSession.builder().master("local").appName("empmgr").getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")
     import spark.implicits._
     
     var empcsv = spark.read.option("header", true)
                    .csv("C:\\Users\\paramesh\\OneDrive\\Documents\\2ndrank1\\2ndrank.csv")
 
 var df1 =empcsv.toDF("empid", "ename", "job", "mgrid", "hiredate", "sal", "comm", "deptno" )
  //var df2 = df1.createOrReplaceTempView("emp")
  //spark.sql("select * from emp").show()
  //spark.sql("""select e.ename AS empname,m.ename AS mgrname from emp e ,emp m
    //where e.mgrid=m.empid""").show()
 df1.show()

 
 var selfjoin =df1.as("emp1").join(df1.as("emp2"))
                          .where(col("emp1.mgrid") === col("emp2.empid"))
                        .select(col("emp1.empid") as ("employee id"),
                            col("emp1.ename") as ("emp name"),
                 
                            col("emp2.empid") as ("Manager Id"),
                            col("emp2.ename") as ("Manager Name")).show()
 
      
      
      
      
      
      
      
      
      
   }
  
}