import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
 import org.apache.spark.sql.SQLContext
object WindowFunction  {
def main(args: Array[String]) {
val spark: SparkSession = SparkSession.builder().master("local").appName("rank").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
import spark.implicits._
var empDF = spark.read.format("csv")
              .option("header",true) .option("delimeter ", ",").option("quote","")
                .csv("C:\\Users\\paramesh\\OneDrive\\Documents\\2ndrank1\\2ndrank.csv")
 empDF.show()
 var df1 =empDF.toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno" )
 df1.show()
 
 
//var window = Window.partitionBy($"deptno","job").orderBy($"sal".desc)
//var partitionWindowRank = rank().over(window)
//var ran=empDF.select($"*", partitionWindowRank as "dense_rank").where($"dense_rank" ===2)
//empDF.show()
//ran.show()
// ran.write.mode("Append").csv("C:\\Users\\paramesh\\OneDrive\\Desktop\\result")
 
 //sql
df1.createOrReplaceTempView("emp")
var df2 = spark.sql("select * from emp").show()
var rank= spark.sql("""select * from 
    (select *,DENSE_RANK() over(partition by deptno order by sal desc) as rn  from emp) """)
             rank.show()
               rank.where($"rn"=== 2).show() 
  }
}
