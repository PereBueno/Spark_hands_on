package com.pbueno.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TotalAmountsSpentByCustomer{

  def parseLine (line: String): (String, Float) = {
    val values = line.split(",")
    (values(0), values(2).toFloat)
  }

  def main(args:Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "AmountsPerCustomer")

    val lines = sc.textFile("src/main/resources/customer-orders.csv")

    val data = lines.map(parseLine)

    val groupedData = data.reduceByKey((x,y) => x+y)

    // Maybe better way of sorting this RDD would be directly building it in the right order

    val sortedData = groupedData.map((x) => (x._2, x._1)).sortByKey()

    sortedData.collect().foreach(println)

  }
}

