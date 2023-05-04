
import scala.io.StdIn.{readLine,readInt}
import scala.math._
import scala.collection.mutable.ArrayBuffer
import java.io.PrintWriter
import scala.io.Source
 

object func {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(466); 
  def executeQuery(dbName:String,tableName:String,date:String):String=
  {
  var rest:String=null
  if(dbName.contentEquals("retail"))
  {
 
  var Quertstr=s"select * from $dbName.$tableName where date=$date >> /hive/hduser/test.csv"
  var rest= s"hive-e $Quertstr"
  
  }
  return rest
  
  };System.out.println("""executeQuery: (dbName: String, tableName: String, date: String)String""");$skip(50); val res$0 = 
  
  executeQuery("retail","customer","20220303");System.out.println("""res0: String = """ + $show(res$0))}
  
 }
