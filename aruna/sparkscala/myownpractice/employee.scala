package myownpractice
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object employee1 {
  def main(args:Array[String]){
  val  spark:SparkSession =SparkSession.builder().master("local[*]").appName("employee1").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._


val empDF = Seq(
  (1, "John Doe", Some(4), 100000),
  (2, "Jane Smith", Some(4), 90000),
  (3, "Bob Johnson", Some(5), 80000),
  (4, "Sarah Lee", Some(5), 70000),
  (5, "David Kim", None, 60000),
  (6, "Lisa Chen", Some(4), 110000),
  (7, "Mike Lee", Some(5), 95000),
  (8, "Emily Park", Some(4), 85000),
  (9, "Tom Brown", Some(5), 120000),
  (10, "Kate Kim", Some(4), 105000)
).toDF("EmployeeID", "EmployeeName", "Managerid", "Salary")





val joinedDF = empDF.alias("empdf")
  .join(empDF.as("mgr"), col("mgr.EmployeeID") === col("empdf.Managerid"), "left")
  .select(col("empdf.EmployeeID"), col("empdf.EmployeeName"), col("empdf.Salary"), col("mgr.Salary").alias("ManagerSalary"))

val resultDF = joinedDF
  .filter(expr("Salary > ManagerSalary"))
  .select("EmployeeName","empdf.Salary","ManagerSalary")
  resultDF.show()


val employeesWhoEarnMoreThanManagers = empDF.alias("e")
  .join(empDF.alias("m"), $"e.Managerid" === $"m.EmployeeID")
  .filter($"e.Salary" > $"m.Salary")
val res =employeesWhoEarnMoreThanManagers .select($"e.EmployeeName",$"e.Salary",$"m.Salary")

employeesWhoEarnMoreThanManagers.show()
res.show()

}
}
 
