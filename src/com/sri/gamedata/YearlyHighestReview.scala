package com.sri.gamedata
import org.apache.spark.sql.SparkSession
import java.lang.Float

object YearlyHighestReview {
  def main(args:Array[String]):Unit = {
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
       (strArr(8).trim(),Tuple2(Float.parseFloat(strArr(5)), strArr(2).trim()))
           }
      }.aggregateByKey(Tuple2(new Float(0.0),""))(
      (a,b) => {
        if(a._1 > b._1) (a._1,a._2) else (b._1,b._2)
      },
      (a,b) => {
        if(a._1 > b._1) (a._1,a._2) else (b._1,b._2)
      }
      ).saveAsTextFile(args(1)) 
   }
} 
















