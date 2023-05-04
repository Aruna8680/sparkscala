
import scala.io.StdIn.{readLine,readInt}
import scala.math._
import scala.collection.mutable.ArrayBuffer
import java.io.PrintWriter
import scala.io.Source
 

object func {
  def executeQuery(dbName:String,tableName:String,date:String):String=
  {
  var rest:String=null
  if(dbName.contentEquals("retail"))
  {
 
  var Quertstr=s"select * from $dbName.$tableName where date=$date >> /hive/hduser/test.csv"
  var rest= s"hive-e $Quertstr"
  
  }
  return rest
  
  }                                               //> executeQuery: (dbName: String, tableName: String, date: String)String
  
  executeQuery("retail","customer","20220303")    //> res0: String = null
  
 }