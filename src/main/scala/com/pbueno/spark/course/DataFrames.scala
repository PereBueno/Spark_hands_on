package com.pbueno.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by POBO on 20/07/2017.
  */
object DataFrames {

  def main (args:Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR);

    val sparkSession = SparkSession
      .builder()
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/tmp") // This line is due to a Spark 2.0.0 bug in windows, uncomment in Linux
      .getOrCreate();

    import sparkSession.implicits._

    val lines = sparkSession.sparkContext.textFile("src/main/resources/fakefriends.csv")
    val people = lines.map(mapper).toDS().cache();

    println("Here is the schema")
    people.printSchema()

    println("Select only names")
    people.select("name").show();

    println("Filtering everybody over 21")
    people.filter(people("age") < 21).show()

    println("Group by age")
    people.groupBy("age").count().orderBy("age").show()

    println("Make everybody 10 years older")
    people.select(people("name"), people("age") + 10).show()

    sparkSession.stop();
  }

  case class Person(ID:Int, name:String, age:Int, numFriends:Int)

  def mapper(line:String):Person = {
    val values = line.split(",");
    val person:Person = new Person(values(0).toInt, values(1).toString, values(2).toInt, values(3).toInt);
    return person;
  }
}
