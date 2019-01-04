package com.sri.gamedata

import org.apache.spark.sql.SparkSession
import java.lang.Float
import java.lang.Double

object OverAllHighest {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

    val gameData = session.read.option("header", true).csv(args(0)).rdd
    val res =
      gameData.filter { line =>
        {
          (line.toString().split(",").length == 11)
        }
      }.map { line =>
        {
          val strArr = line.toString().split(",")
          val title = strArr(2)
          var score = 0.0
          try {
            score = Double.parseDouble(strArr(5))
          } catch {
            case e: NumberFormatException => None
          }
          val year = strArr(8)
          ((year, title), (score, 1))
        }
      }.aggregateByKey(Tuple2(0.0, 0))(
        (acc, nextVal) => (acc._1 + nextVal._1, acc._2 + nextVal._2),
        (acc, nextVal) => (acc._1 + nextVal._1, acc._2 + nextVal._2)).mapValues { v =>
          {
            var avg = 0.0
            if (v._2 != 0) avg = v._1 / v._2
            avg
          }
        }.aggregateByKey(0.0)(
            (acc,nextVal) => { if( acc > nextVal) acc else nextVal},
            (acc,nextVal) => {if (acc > nextVal) acc else nextVal}
            ).map{ x => {(x._1._1,(x._1._2,x._2))}}.saveAsTextFile(args(1))
  }
}  



//,score_phrase,title,url,platform,score,genre,editors_choice,release_year,release_month,release_day