package com.pbueno.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by POBO on 21/02/2017.
  */
object FriendsByAge {

  // Defining a method to parse the fields we need from the csv
  def parseLine(line: String) : (Int, Int)={
    val splittedValues = line.split(",")
    (splittedValues(2).toInt, splittedValues(3).toInt)
  }

  def main(args:Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR);

    // Creating the spark context

    val sc = new SparkContext("local[*]", "FindFriendsByAge")

    // Load the file into an RDD
    val lines = sc.textFile("src/main/resources/fakefriends.csv")

    // Get the tuples we need into another RDD
    val rdd = lines.map(parseLine)

    // MAGIC!!!!
    // This will return (sum of friends for each age, number of records added for this age)
    val totalsByAge = rdd.mapValues(x => (x,1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    // For each age we'll have a tuple (sum of friends, total people), so dividing we have it
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

    // Triggering the collect action, to get all the data from the RDD (remember it might be distributed)
    val result = averagesByAge.collect()

    result.sorted.foreach(println)

  }
}
