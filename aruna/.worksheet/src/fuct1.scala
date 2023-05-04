object fuct1 {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(50); 
  def getSum2(args: Int*) : Int = {;System.out.println("""getSum2: (args: Int*)Int""");$skip(20); 
  var sum : Int = 0;System.out.println("""sum  : Int = """ + $show(sum ));$skip(37); val res$0 = 
  for(sum <- args){
    sum =sum*num
  };System.out.println("""res0: <error> = """ + $show(res$0));$skip(10); val res$1 = 
  sum;System.out.println("""res1: Int = """ + $show(res$1))}
}
 
println("getSum2: " + getSum2(1,2,3,4,5))
