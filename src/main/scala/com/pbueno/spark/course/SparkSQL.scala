package com.pbueno.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by POBO on 19/07/2017.
  */
object SparkSQL {
  def main (args:Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR);

    val sparkSession = SparkSession
      .builder()
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/tmp") // This line is due to a Spark 2.0.0 bug in windows, uncomment in Linux
      .getOrCreate();

    val lines = sparkSession.sparkContext.textFile("src/main/resources/fakefriends.csv");
    val people = lines.map(mapper); // RDD with Person objects

    // Converting to DataSet --> Beware this import, it's needed
    import sparkSession.implicits._
    val dataSet = people.toDS;

    dataSet.printSchema() // print the schema
    dataSet.createOrReplaceTempView("people") // create a View, now we can query it using plain SQL from out session

    sparkSession.sql("select * from people where age >= 13 and age <= 19").collect().foreach(println)

    // Let's close connection
    sparkSession.stop()
  }

  case class Person(ID:Int, name:String, age:Int, numFriends:Int)

  def mapper(line:String):Person = {
    val values = line.split(",");
    val person:Person = new Person(values(0).toInt, values(1).toString, values(2).toInt, values(3).toInt);
    return person;
  }
}
