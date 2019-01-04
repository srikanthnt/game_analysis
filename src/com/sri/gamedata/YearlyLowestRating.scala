package com.sri.gamedata

import org.apache.spark.sql.SparkSession
import java.lang.Float


object YearlyLowestRating {
  def main(args:Array[String]):Unit = {
    val dummyRating=99.9
    val dummyString=""
    val session=SparkSession
                .builder()
                .appName("YearlyReview")
                .getOrCreate()
                
    val gameData=session.read.option("header", true).csv(args(0)).rdd
    val res=gameData.filter{line=>{
      (line.toString().split(",").length == 11)
                }
          }.map{line => {
       val strArr=line.toString().split(",")
       (strArr(8).trim(),new Tuple2(Float.parseFloat(strArr(5)), strArr(2).trim()))
           }
      }.aggregateByKey(new Tuple2(dummyRating,dummyString))(
      (a,b) => {
        if(a._1 < b._1) (a._1,a._2) else (b._1,b._2)
      },
      (a,b) => {
        if(a._1 < b._1) (a._1,a._2) else (b._1,b._2)
      }
      ).mapValues{v =>  (v._2,v._1) }.saveAsTextFile(args(1))
   }
  
}
      
      
      
      
      
      
      
      
      