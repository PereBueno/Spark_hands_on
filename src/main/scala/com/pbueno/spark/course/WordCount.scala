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

    // Simple splitting, by spaces
    //val words = lines.flatMap(x => x.split(" "))
    // Better splitting, using ER & lowercase to ignore caps
    val words = lines.flatMap((x => x.split("\\W+")))

    val lowerCaseWords = words.map((x => x.toLowerCase()))

    // 2 ways for sorting this
    // 1) Simply count the collection of words
    //val wordCount = words.countByValue()
    // 2) Map-reducing to count and then reversing the values order
    // Although solution 1 is simpler, it's a sequence, so it can not be executed distributed, while the second option
    // works with a RDD, that can be shared across nodes
    val wordCounts = lowerCaseWords.map(x => (x,1)).reduceByKey((x,y) => x + y)
    val wordCountsSorted = wordCounts.map(x => (x._2, x._1)).sortByKey()

    for (result <- wordCountsSorted){
      println (result._2 + ": " + result._1)
      /*val count = result._1
      val word = result._2
      println(s"$word: $count")*/
    }
    //sortedWordCount.foreach(println)
  }
}
