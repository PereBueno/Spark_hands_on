package com.pbueno.spark.course

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import scala.io.{Codec, Source}

/**
  * Created by POBO on 20/07/2017.
  */
object PopularMoviesDataSets {

  def loadMovieNames() : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("src/main/resources/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames
  }

  final case class Movie(movieID:Int)

  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR);

    val sparkSession = SparkSession
      .builder()
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/tmp") // This line is due to a Spark 2.0.0 bug in windows, uncomment in Linux
      .getOrCreate();

    val lines = sparkSession.sparkContext.textFile("src/main/resources/u.data").map(x => Movie(x.split("\t")(1).toInt))

    import sparkSession.implicits._ // Needed, as always
    val movieDS = lines.toDS();

    // Let's sort movies by popularity. This is the real save, with only one line you simplify all the logic that was quite complex with RDDs
    // These lines
    //    val movieCounts = movies.reduceByKey( (x, y) => x + y )
    //    val flipped = movieCounts.map( x => (x._2, x._1) )
    //    val sortedMovies = flipped.sortByKey()
    // Get reduced to a simple
    //    movieDS.groupBy("movieId").count().orderBy(desc("count"))
    // Much more readable
    // Also, later assignation to an entry in the names map is much easier

    val topMoviesIds = movieDS.groupBy("movieId").count().orderBy(desc("count")).cache() // note desc is part of sql.functions package

    // We list top 20
    topMoviesIds.show()

    // Let's see the top 10
    val top10 = topMoviesIds.take(10);

    // Movie's names, in a different file, represented as a map
    val names = loadMovieNames()

    println("Top 10")
    for (result <- top10){
      // result is just a row, to print it we must cast it back
      println(names(result(0).asInstanceOf[Int]) + ": " + result(1))
    }

    sparkSession.stop();
  }
}
