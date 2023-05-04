import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object readjson {
  def main(args : Array[String]): Unit = {
    
   var conf =new SparkConf().setAppName("read json app").setMaster("local[*]")
   val sc = new SparkContext(conf)
   val sqlContext = new SQLContext(sc)
   var schemapath=args(0)
   var datapath=args(1)
   var resultpath=args(2)
   
    var readschema = sqlContext.read.json(schemapath).schema
 var readData = sqlContext.read.schema(readschema).format("json").json(datapath)
 .where("events.beaconType = 'pageAdRequested' ")
val pageAdRequested = """events.client AS clients
  events.beaconType AS beaconType
  events.data.displayAd.indexExchangeHB AS  indexExchangeHB
  events.data.displayAd.instanceID AS  instanceID
  events.data.milestones.amazonA9Requested AS  amazonA9Requested
  events.data.milestones.amazonA9Received AS  amazonA9Received
  events.data.milestones.adRequested AS  adRequested """.split("\n")
  
val flattendDF= readData.selectExpr(pageAdRequested:_*)
val beaconType=pageAdRequested
flattendDF.createOrReplaceTempView("test")
  //flattendDF.coalesce(1).write.mode("overWrite").partitionBy("beaconType").option("header", "true").format("csv").save("D:\\aru")//
  flattendDF.coalesce(1).write.mode("overWrite").partitionBy("beaconType").save(resultpath)
 
  }
}
