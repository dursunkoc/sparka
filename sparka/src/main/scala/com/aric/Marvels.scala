package com.aric

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dursun on 11/17/16.
  */
object Marvels {

  def main(args: Array[String]) {
    val booksPath = "/Users/dursun/workspaces/spark_ws/data/books.txt"
    val characterPath = "/Users/dursun/workspaces/spark_ws/data/characters.txt"
    val edgesPath = "/Users/dursun/workspaces/spark_ws/data/edges.txt"


    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setAppName("marvel app")
    sparkConf.setMaster("local[8]")

    val sc: SparkContext = new SparkContext(sparkConf)
    val booksRDD = sc.textFile(booksPath)
    val charactersRDD = sc.textFile(characterPath)
    val edgesRDD = sc.textFile(edgesPath)

    val edgeFilter = (line: String) => !(line.contains("*") || line.contains("\""))
    val edgesFiltered: RDD[String] = edgesRDD.filter(edgeFilter)
    val characterBookMap = edgesFiltered.
      map(l => l.split(" ")).
      map(xs => (xs.head, xs.tail))

    val charactersPRDD = charactersRDD.map(l => l.split(":")).map(xs => (xs(0).substring(7), xs(1).trim))
    val characterLookUp = charactersPRDD.collectAsMap()
    println("--------------")
    val characterStrength = characterBookMap.mapValues(x => x.length).reduceByKey((acc, n) => acc + n)
      .map(t => (characterLookUp(t._1), t._2))
    characterStrength.sortBy(t => t._2, false).take(10).foreach(println)

    val bookCharacterMap = characterBookMap.flatMapValues(t => t).map(t => (t._2, t._1)).reduceByKey((acc, n) => acc + "," + n)
      .mapValues(s => s.split(","))

    val coocuranceMap = bookCharacterMap.flatMap(t => t._2.map((t._1, _)))

    val coocuranceStrenght = coocuranceMap.map(t => (t, 1)).reduceByKey(_ + _)

    coocuranceStrenght.map(t=>(t._1._1,t._1._2,t._2)).sortBy(t=>t._3, false).take(10).
      foreach(println)
    //foreach(t=>println(characterLookUp(t._1),characterLookUp(t._2),t._3))
/*      map(t=>(
        characterLookUp(t._1),
        characterLookUp(t._2),
        t._3
        )
      ).foreach(println)
*/
  }
}
