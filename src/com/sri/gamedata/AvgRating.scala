package com.sri.gamedata
//first git commit

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.lang.Float

object AvgRating {
  def main(args: Array[String])= {
    val session=SparkSession
                .builder()
                .appName("gamedata")
                .getOrCreate()
    val dat=session.read.csv(args(0)).rdd
    val res=dat.filter{ line =>{
      (line.toString().split(",").length == 11)
      }
  }.map{line => {
                 val strArr=line.toString().split(",")
                 var value=0.0f
                 try{
                   value=Float.parseFloat(strArr(5))
                 }
                 catch
                 {
                   case e : NumberFormatException => None
                 }
                 (strArr(2),value)
              }
              
            }.mapValues{v =>{
              (v,1)
              }
            }.reduceByKey{(a,b) => {
              (a._1 + b._1 , a._2 + b._2)          
            }
            }.mapValues{v => {
              (v._1/v._2)
            }
            
           }.saveAsTextFile(args(1))
        }
}
 





























