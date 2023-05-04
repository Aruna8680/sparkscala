object fuct1 {
  def getSum2(args: Int*) : Int = {
  var sum : Int = 0
  for(sum <- args){
    sum =sum*num
  }
  sum
}
 
println("getSum2: " + getSum2(1,2,3,4,5))