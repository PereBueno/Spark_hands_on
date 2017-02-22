package com.pbueno.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by POBO on 22/02/2017.
  */
object WordCount {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR);

    // Creating the spark context
    val sc = new SparkContext("local[*]", "FindFriendsByAge")

    // Load the file into an RDD
    val lines = sc.textFile("src/main/resources/book.txt")

    val words = lines.flatMap(x => x.split(" "))

    val wordCount = words.countByValue()

    wordCount.foreach(println)
  }
}
