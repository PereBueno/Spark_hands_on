package com.pbueno.spark.course

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}
import scala.math.sqrt

/**
  * Created by POBO on 01/03/2017.
  */
object MovieSimilarities {

  // Internal types
  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))
  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  // Creates a map with movieId -> Movie title
  def refDataLoader() : Map[Int, String] = {
    // Hnadling character enconding issues
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var data = Map[Int, String]()
    val lines = Source.fromFile("../src/main/resources/u.item").getLines()

    for (line <- lines){
      val fields = line.split('|')
      if (fields.length > 1){
        data += (fields(0).toInt -> fields(1).trim)
      }
    }
    return data
  }

  def filterDuplicates(ratings:UserRatingPair):Boolean = {
    return ratings._2._1._1 < ratings._2._2._1
  }

  def makeMoviePairs(userRating:UserRatingPair): ((Int, Int), (Double, Double)) ={
    // We get the 2 ratings for each row, so we generate pairs ((movie1, movie2), (rating1, rating2))
    ((userRating._2._1._1, userRating._2._2._1), (userRating._2._1._2, userRating._2._2._2))
  }

  /**
    * This is a really cool way of computing similarity, by representing each rating as a graphic and the
    * checking the cosine
    * @param ratingPairs
    * @return
    */
  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)

    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }

    return (score, numPairs)
  }

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MovieSimilarities")

    val movieNames = refDataLoader()

    val data = sc.textFile("../src/main/resources/u.data")

    // Creating an RDD with format (userId, (movieId, Rating))
    val ratings = data.map (l => l.split("\t")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))

    // Join ratings with itself, so it can join values with the same key (movies rated by the same user)
    // Keep in mind that join returns every possible permutation, so we'll get (e.g.)
    // User 1 watched movieA & movieA, movieA & movieB, movieA & movieC, movieB & movieA, movieB & movieB,
    // movieB & movieC, movieC & movieA ....
    val joinedRatings = ratings.join(ratings)

    // We remove dups, so every entry like  (movieA, movieA) will be filtered
    val uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

    // What we really want is the movie info, so we supress the user to get pairs of (moviepair, ratingpair)
    val moviePairs = uniqueJoinedRatings.map(makeMoviePairs)

    // We want to get similar movies, so we're collecting by key, to get all the avg ratings for a given pair of movies
    val moviePairRating = moviePairs.groupByKey()

    // Now that we have (movieA, movieB) => [(ratA, ratB), (ratA, ratB)...] we can compute the similarity.
    // We only care about the value here, not the key, so we use mapValues instead of map
    // Also, we're caching the result, as we'll use it later more than once
    // With this we'll get an RDD with (moviePair, (similarity, # of occurrences))
    val moviePairSimilarities = moviePairRating.mapValues(computeCosineSimilarity).cache()

    // The movie to look for will be passed as an argument to the program

    if (args.length > 0){
      val movieId: Int = args(0).toInt

      // We define two tresholds:
      // One to indicate the score, above that value we consider a movie to be similar
      // One to indicate number of times those movies were rated together
      val scoreTreshold = 0.97
      val occurrenceTreshold = 50

      // We filter results that don't match our tresholds
      val filteredResults = moviePairSimilarities.filter( x => {
        val pair = x._1
        val simValues = x._2

        (pair._1 == movieId || pair._2 == movieId) && // One of the movies is the searched one
          (simValues._1 > scoreTreshold) &&           //Rating calculated by our function is higher than treshold
          (simValues._2) > occurrenceTreshold         // Both movies are rated together more than treshold times

      })

      // We now need to sort the results, so we
      // 1) Flip the pairs
      // 2) Sort by key (using false to indicate descending order)
      // 3) Get only the top 10

      val topResults = filteredResults.map(x => (x._2, x._1)).sortByKey(false).take(10)

      println (s"Showing top similarities for movie $movieId: " + movieNames(movieId))
      for (result <- topResults){
        // Get the movie that is not the base one
        var similarOne = result._2._1
        if (similarOne == movieId){
          similarOne = result._2._2
        }
        println( movieNames(similarOne) + " (" + similarOne+"): voted " + result._1._2 + " times, with a " + (result._1._1 * 100) + "% of coincidence")
      }
    }
  }
}
