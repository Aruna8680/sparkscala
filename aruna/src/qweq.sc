

import scala.io.StdIn.{readLine,readInt}
import scala.math._
import scala.collection.mutable.ArrayBuffer
import java.io.PrintWriter
import scala.io.Source


object qweq {
 
  var randSent="I saw a dragon fly by"            //> randSent  : String = I saw a dragon fly by
  println("String length " + randSent.length())   //> String length 21
  println(randSent.concat(""))                    //> I saw a dragon fly by
println("Are strings equal " + "aru".equals(randSent))
                                                  //> Are strings equal false
println("dragon starts at index ", randSent.indexOf("dragon"))
                                                  //> (dragon starts at index ,8)
val randSentArray = randSent.toArray              //> randSentArray  : Array[Char] = Array(I,  , s, a, w,  , a,  , d, r, a, g, o, 
                                                  //| n,  , f, l, y,  , b, y)
  randSent.toCharArray()                          //> res0: Array[Char] = Array(I,  , s, a, w,  , a,  , d, r, a, g, o, n,  , f, l,
                                                  //|  y,  , b, y)





}