package com.pbueno.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

/**
  * Another Machine Learning example, this time using dataFrames instead of RDDs, please note that objects correspond
  * to the ml package, instead of the mllib one, as that's where the not-deprecated packages are
  * Created by POBO on 27/07/2017.
  */
object LinearRegressionDataFrames {
  def main (args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession
      .builder()
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/tmp") // This line is due to a Spark 2.0.0 bug in windows, uncomment in Linux
      .getOrCreate();

    val inputLines = sparkSession.sparkContext.textFile("src/main/resources/regression.txt")
    // Create the data. In ML language, label is the value you're trying to predict, while feature is the data you use to make
    // a prediction. As you can have more than one feature (more accurate) you use a Vector for features
    // As in LinearRegression previous example, we're using the same data for the model and the prediction
    val inputData = inputLines.map(_.split(",")).map(x => (x(0).toDouble, Vectors.dense(x(1).toDouble)))

    // Here we see a different way of creating a dataframe, instead of defining a case class (@see PopularMoviesDataSets)
    // We define a sequence with column names and create a dataframe using colNames: _* (use the objects in the provided list)
    import sparkSession.implicits._
    val colNames = Seq ("label", "features")
    val df = inputData.toDF(colNames: _*)

    // We'll split the data into 2 sets, one to train the algorithm and the other one to test it, so we'll have 50% of the data only
    val splitDf = df.randomSplit(Array(0.5, 0.5))
    val trainDf = splitDf(0)
    val testDf = splitDf(1)

    // Creating the linear regression model
    val lir = new LinearRegression()
      .setElasticNetParam(0.8)  // elastic net mixing
      .setRegParam(0.3) // regularization
      .setMaxIter(100) // max iterations
      .setTol(1E-6) // convergence tolerance

    val model = lir.fit(trainDf)
    // using our trained model we'll add a prediction column to our dataframe
    val fullPredictions = model.transform(testDf).cache()

    // As it's a dataframe we can easlity use a select over the wanted columns
    // Note that we use getDouble instead of toDouble, as we work with a row object now
    val predictionsAndLabels = fullPredictions.select("prediction", "label").map(x => (x.getDouble(0),x.getDouble(1) ))

    for (pred <- predictionsAndLabels){
      println(pred)
    }

    sparkSession.stop()

  }
}
