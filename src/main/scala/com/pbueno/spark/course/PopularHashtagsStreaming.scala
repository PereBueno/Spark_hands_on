package com.pbueno.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source

/**
  * Created by POBO on 09/08/2017.
  */
object PopularHashtagsStreaming {
  /**
    * Setting up logging, to avoid spam
    */
  def loggingSetup(): Unit ={
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  /**
    * Setting up twitter authetication. Obviously, never upload the credentials file anywhere
    */
  def twitterSetup(): Unit ={
    for (line <- Source.fromFile("src/main/resources/twitter.txt").getLines()){
      val fields = line.split(" ")
      if (fields.length == 2)
        System.setProperty("twitter4j.oauth." + fields(0), fields(1));
    }
  }

  def main(args:Array[String]): Unit ={
    twitterSetup()

    // Setup a spark streaming context, it will ingest data every second
    val ssc = new StreamingContext("local[*]", "Popular hastags", Seconds(1))
    // Must be called once the context is created
    loggingSetup()

    // Create the Dstream with tweets, note auth is set as None because we set it up as a system prop
    val tweets = TwitterUtils.createStream(ssc, None)

    // We're going to get every tweet, split it into words, filter only hastags and create a key value to aggregate
    val hastagKeyValues = tweets.map(status => status.getText()).flatMap(line => line.split(" "))
      .filter((word => word.startsWith("#"))).map(hash => (hash, 1));

    // Now we reduce by key with a window, collecting 5 minutes of tweets. Note that windowed functions also take a 2nd
    // (optional) parameter specifying what to do when something is moved out of the window.
    val counts = hastagKeyValues.reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(10))

    // We sort the results
    val sortedResults = counts.transform(rdd => rdd.sortBy(x => x._2, false))

    // And print top 10
    sortedResults.print()

    // We setup a checkpoint directory, where temp data is stored, so it can continue in case of failure
    ssc.checkpoint("c:/tmp/checkpoint/")
    // And launch it!
    ssc.start()
    ssc.awaitTermination()

  }
}
