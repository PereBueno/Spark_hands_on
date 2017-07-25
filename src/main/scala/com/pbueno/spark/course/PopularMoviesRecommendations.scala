package com.pbueno.spark.course

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}
import org.apache.spark.mllib.recommendation.{ALS, Rating}

/**
  * Created by POBO on 25/07/2017.
  */
object PopularMoviesRecommendations {

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

  def main(args:Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR);

    val sc = new SparkContext("local[*]", "PopularMovieRecommendations")

    val nameDict = loadMovieNames()

    val data =  sc.textFile("src/main/resources/u.data")

    // We map to a MLLib Rating object,
    val ratings = data.map(x => x.split("\t")).map(x => Rating(x(0).toInt, x(1).toInt, x(2).toDouble)).cache()

    val rank = 8
    val numIterations = 20

    val model = ALS.train(ratings, rank, numIterations)

    val userId= 23
    println("Ratings for user " + userId)
    val userRatings = ratings.filter(x => x.user == userId).collect()

    for (rating <- userRatings){
      println(nameDict(rating.product.toInt) + ": " + rating.rating.toDouble)
    }

    println ("\nTop 10 recommendations")
    val recommendations = model.recommendProducts(userId, 10)

    for (rating <- recommendations){
      println(nameDict(rating.product.toInt) + ": " + rating.rating.toDouble)
    }

  }
}
