package com.aric

import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * Created by dursun on 11/15/16.
  */
object Dodgers {

  val fmtDateTime: DateTimeFormatter = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm");
  val fmtDay: DateTimeFormatter = DateTimeFormat.forPattern("MM/dd/yyyy");
  val fmtDayShortYear: DateTimeFormatter = DateTimeFormat.forPattern("MM/dd/yy");

  val checkGameDay: ((DateTime, (Int, Option[String]))) => ((DateTime, Option[String], String, Int)) =
    (t: (DateTime, (Int, Option[String]))) => {
      t._2._2 match {
        case None => (t._1, t._2._2, "Regular Day", t._2._1)
        case o: Some[String] => (t._1, t._2._2, "Game Day", t._2._1)
      }

    }

  var cc: (Int) => (Int, Int) = (v: Int) => (v, 1)

  var mv: ((Int, Int), Int) => (Int, Int) = (t: (Int, Int), c: Int) => ((t._1 + c), (t._2 + 1))

  var mc: ((Int, Int), (Int, Int)) => (Int, Int) = (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t1._2, t2._1 + t2._2)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]")
    sparkConf.setAppName("Dodgers App")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    val dataRdd = sparkContext.textFile("file:/Users/dursun/workspaces/spark_ws/data/Dodgers.data.txt")
    val eventRdd = sparkContext.textFile("file:/Users/dursun/workspaces/spark_ws/data/Dodgers.events.txt")

    val mappedDataRdd = dataRdd
      .map(s => s.split(","))
      .map(xs => (DateTime.parse(xs(0), fmtDateTime), xs(1).toInt))
      .map(t => (fmtDay.print(t._1), t._2))
      .map(t => (DateTime.parse(t._1, fmtDay), t._2))
      .reduceByKey((acc, n) => acc + n)
      .sortBy(t => t._2, false)
    //    mappedDataRdd.take(10).foreach(println)

    val mappedEventRdd = eventRdd
      .map(s => s.split(","))
      .map(xs => (DateTime.parse(xs(0), fmtDayShortYear), xs(4)))
    //    mappedEventRdd.take(10).foreach(println)

    val jointRdd = mappedDataRdd.leftOuterJoin(mappedEventRdd)
      .map(checkGameDay).sortBy(t => t._4, false)
      .map(t => (t._3, t._4))
      .combineByKey(cc, mv, mc)
      .map(t=>(t._1, t._2._1/t._2._2))

    jointRdd.foreach(println)
  }
}
