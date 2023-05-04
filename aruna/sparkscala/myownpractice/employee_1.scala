package myownpractice
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object employee_1 {
  def main(args:Array[String]){
  val  spark:SparkSession =SparkSession.builder().master("local").appName("employee_1").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  
  val empdf =spark.read.format("csv").option("header",true).option("Inferschema",true).
                                csv("C:\\Users\\paramesh\\OneDrive\\Desktop\\newcsv\\employee.csv").toDF()
  val empdf1 =spark.read.format("csv").option("header",true).option("Inferschema",true).
                                csv("C:\\Users\\paramesh\\OneDrive\\Desktop\\newcsv\\Employee_dept.csv").toDF()
   empdf1.show()                             
 val deptdf =spark.read.format("csv").option("header",true).option("Inferschema",true).
                                csv("C:\\Users\\paramesh\\OneDrive\\Desktop\\newcsv\\Department.csv").toDF()                               
    deptdf.show()
  //EmployeeID|EmployeeName|Salary2|DepartmentID| Department|ManagerID|Salary6|
 
  //return the top 10 highest-paid employees
  val top_10 = empdf.orderBy(col("Salary2").desc).as("employeesalary").limit(10)
 // top_10.show()
  
  //find the total number of employees in each department
 
   val tot_emp = empdf.groupBy("Department", "EmployeeID").count()
   val tot_emp_by_dept = tot_emp.groupBy("Department").agg(sum("count").as("Total"))
   //tot_emp_by_dept.show()

   val tot_emp1 = empdf.groupBy("Department").count()
   //tot_emp1.show()

  //Given an Employee table with columns EmployeeID, EmployeeName, ManagerID, and Salary, 
    //write query to return the names of all employees who earn more than their managers.
   val joindf=empdf.alias("emp").join(empdf.alias("mgr") , col("mgr.EmployeeID") === col("emp.ManagerID"),"left").
                    select(col("emp.EmployeeID"),col("emp.EmployeeName"),col("emp.Salary2"),
                                               col("mgr.Salary6").as("Managersalary"))
   val result = joindf.filter(expr("Salary2 >Managersalary"))
                .select("emp.EmployeeName","emp.Salary2","Managersalary")

  
  //17.	Given an Employee table with columns EmployeeID, EmployeeName, and DepartmentID, and a Department table with columns DepartmentID and DepartmentName, 
  //write query to return the names of all employees and their departments.
                
  //EmployeeID|EmployeeName|DepartmentID| -----DepartmentID| DepartmentName|
  
    val join =empdf1.join(deptdf,Seq("DepartmentID"))
    val Names = join.select("EmployeeName","DepartmentName").show()

  
  

}
}