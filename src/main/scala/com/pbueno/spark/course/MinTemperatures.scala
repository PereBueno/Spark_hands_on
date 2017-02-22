package com.pbueno.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * Created by POBO on 22/02/2017.
  */
object MinTemperatures {

    def parseLine(line: String) : (String, String, Float) = {
      val fields = line.split(",")
      // Data comes in the format Station_id, date, type of metric, value, in the case of temperature it's 10ths of a cº degree
      // We will output the data in Farenheit degrees
      (fields(0), fields(2), fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f)
  }

  def mainForMins (args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Minimum temperatures")

    val lines = sc.textFile("src/main/resources/1800.csv")

    val parsedLines = lines.map(parseLine)

    val minTemps = parsedLines.filter(x => x._2 == "TMIN")

    // Everything is TMIN, so we don't need it anymore, reducing to a 2-tuple
    val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))

    val minByStation = stationTemps.reduceByKey((x,y) => math.min(x, y))

    val results = minByStation.collect()

    for (result <- results.sorted){
      val fmtTemp = f"${result._2}%.2f F"
      println (result._1 + " min temperature: " + fmtTemp)
    }
  }

  def main (args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Minimum temperatures")

    val results = sc.textFile("src/main/resources/1800.csv").map(parseLine).filter(x => x._2 == "TMAX").map(x => (x._1, x._3.toFloat))
      .reduceByKey((x,y) => math.max(x, y)).collect()


    for (result <- results.sorted){
      val fmtTemp = f"${result._2}%.2f F"
      println (result._1 + " min temperature: " + fmtTemp)
    }
  }
}
