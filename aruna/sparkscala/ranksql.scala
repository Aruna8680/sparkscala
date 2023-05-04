 import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
 import org.apache.spark.sql.SQLContext


object ranksql {
    def main(args: Array[String]) {

  val spark: SparkSession = SparkSession.builder() .master("local").appName("rank").getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val simpleData = Seq(("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100))
  var  df = simpleData.toDF("Name", "dept", "sal")
 
  df.createOrReplaceTempView("Employee")
  spark.sql("select * from Employee")
 var rank = spark.sql("""select * from 
( select *, DENSE_RANK() over (partition by dept order by sal DESC) as 2ndsalary
 from Employee )""")
 rank.show()
 rank.where($"2ndsalary"=== 2).show()
     }
  }
