package com.pbueno.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.optimization.{LBFGS, SquaredL2Updater}
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}

/**
  * Created by POBO on 26/07/2017.
  */
object LinearRegression {

  def main (args:Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Linear regression")

    // Creating 2 RDDs, one for training the algorithm and a second one to test the results, in this case
    // we'll use the same file for both algorithms, so results should be quite accurate
    val trainingLines =  sc.textFile("src/main/resources/regression.txt")
    val testingLines =  sc.textFile("src/main/resources/regression.txt")

    // We need RDDs to contain labeled points to use this algorithm
    // Note we're not caching test data, as it's supposed to change with each execution
    val trainingData = trainingLines.map(LabeledPoint.parse).cache()
    val testingData = trainingLines.map(LabeledPoint.parse)

    // Creating the linear regression model
    // Note this method is deprecated, just added it for comparing
    val algorithm = new LinearRegressionWithSGD()
    algorithm.optimizer
      .setNumIterations(100)
      .setStepSize(1.0)
      .setUpdater(new SquaredL2Updater())
      .setRegParam(0.01)

    // This is the current valid method
    val alg2 = new LinearRegression
    alg2.setRegParam(0.01)
      .setMaxIter(100)

    val model = algorithm.run(trainingData)

    // Predict value for our test data
    val predictions = model.predict(testingData.map(_.features))
    // Zip real values with predicted ones so we can compare
    val predictionsAndLabel = predictions.zip(testingData.map(_.label))

    //print
    for (pred <- predictionsAndLabel){
      println (pred)
    }
  }
}
