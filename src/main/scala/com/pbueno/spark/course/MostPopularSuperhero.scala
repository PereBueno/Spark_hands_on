package com.pbueno.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by POBO on 28/02/2017.
  */
object MostPopularSuperhero {

  def countOcurrences (line: String): (Int, Int) = {
    val values = line.split("\\s+")
    (values(0).toInt, values.length -1)
  }
  // parseNames returns an optional result, with a tuple (id, name) if line is parseable (Scala Some) or nothing (Scala None) if not
  def parseNames (line: String) : Option[(Int, String)] = {
    val values = line.split("\"")
    if (values.length > 1){
      // We found an expected format line
      return Some(values(0).trim.toInt, values(1))
    }else{
      return None // flatMap will discard these lines
    }
  }

  def main (args:Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MostPopularSuperhero")

    val names = sc.textFile("src/main/resources/Marvel-names.txt")

    // Using flatMap instead of map, as in the names file there will for sure be some empty / non-parseable line, so
    // it won't be a 1 to 1 correspondence
    val namesRdd = names.flatMap(parseNames)

    val lines = sc.textFile("src/main/resources/Marvel-graph.txt")

    val pairings = lines.map(countOcurrences)

    // Superheroes can appear more than once, so we have to reduce by key and add each result
    val totalConnections = pairings.reduceByKey((x,y) => x+y)

    val connectionKeyed = totalConnections.map( x => (x._2, x._1))

    val mostPopular = connectionKeyed.max()

    // Lookup for the name of that id in the names RDD
    val mostPopularName = namesRdd.lookup(mostPopular._2)(0)

    println(s"$mostPopularName is the most popular, with ${mostPopular._1} co-appearences")
  }
}
