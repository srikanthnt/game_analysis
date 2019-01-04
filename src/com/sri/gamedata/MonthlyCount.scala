package com.sri.gamedata
import org.apache.spark.sql.SparkSession

object MonthlyCount {
  def main(args:Array[String]):Unit ={
    val session=SparkSession
                .builder()
                .appName("YearlyCount")
                .getOrCreate()
     
    val gameData= session.read.option("header", true).csv(args(0)).rdd
    val res=gameData.filter{ line => {
      (line.toString().split(",").length == 11)
                       }
              }.map{line =>{
                val strArr=line.toString().split(",")
                (strArr(9).trim(),1)
              }
        }.reduceByKey{
          (_ + _)
        }.sortByKey(true).saveAsTextFile(args(1))
    }
}