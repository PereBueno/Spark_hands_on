package com.pbueno.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by POBO on 23/02/2017.
  */
object PopularFilms {

  def main (args:Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext ("local[*]", "PopularMovies")

    val movies = sc.textFile("src/main/resources/u.data")

    val rated = movies.map(x => (x.split("\t")(1),1)).reduceByKey((x, y) => x + y)

    val list = rated.map(x => (x._2, x._1)).sortByKey().collect().foreach(println)
  }
}
