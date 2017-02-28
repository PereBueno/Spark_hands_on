package com.pbueno.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer
import scala.tools.cmd.Spec.Accumulator

/**
  * Created by POBO on 28/02/2017.
  */
object SuperHeroDegreesOfSeparationBFS {

  var startCharacter = 5306   // Spider-man, but declared as var so we can change it
  var targetCharacter = 13    // ADAM, declared as var too

  // It would be definitely better to use ints here, as it would simplify the reducer, but for training purpouses let's keep it as strings
  val EXPLORING:String = "GRAY"    // GRAY in BFS
  val UNEXPLORED:String = "WHITE"  // WHITE in BFS
  val EXPLORED:String = "BLACK"      // BLACK in BFS

  var hitCounter : Option[LongAccumulator] = None // The accumulator, as optional so we can start with None and construct it when needed

  // Custom types to implement BFS
  // BFSData contains tuples of (list of connected nodes, distance, status)
  type BFSData = (Array[Int], Int, String)
  // BFSNode is a tuple of (currentId, BFSData), so for every given id it contains: all connections, current distance and status
  type BFSNode = (Int, BFSData)

  def line2BFS (line:String) : BFSNode ={
    val values = line.split("\\s+")

    val heroId = values(0).toInt

    var connections : ArrayBuffer[Int] = ArrayBuffer()
    for (conn <- 1 to (values.length -1)){
      connections += values(conn).toInt
    }
    // We set initial values of the node, UNEXPLORED and infinite (9999) distance
    var status = UNEXPLORED
    var distance = 9999

    // Covering the base case, scanned node is start character, 1st case only (also covers dups)
    if (heroId == startCharacter){
      status = EXPLORING
      distance = 0
    }
    // Returning the node
    (heroId, (connections.toArray, distance, status))
  }

  /**
    * Expands a simple BFS node into an array containing all connected nodes
    * @param node
    * @return
    */
  def bfsMap (node:BFSNode): Array[BFSNode] = {
    val charId: Int = node._1
    val connections: Array[Int] = node._2._1
    val distance: Int = node._2._2
    var status: String = node._2._3

    // Create an array buffer to store all the possible connections
    var results : ArrayBuffer[BFSNode] = ArrayBuffer()

    // If the node is currently in EXPLORING status we create a new node with same status for each connection
    if (EXPLORING == status){
      for (conn <- connections){
        val newId = conn
        val newDist = distance + 1
        val newStatus = EXPLORING

        // if it's the target char, add a hit to the global accumulator
        if (targetCharacter == conn){
          if (hitCounter.isDefined){
            hitCounter.get.add(1)
          }
        }
        // Add new node to the buffer
        val newEntry: BFSNode = (newId, (Array(), newDist, newStatus))
        results += newEntry
      }
      status = EXPLORED
    }

    // We add the original node to the results, to avoid losing the connection. We set it to  explored, so it won't get scanned again
    val original: BFSNode = (charId, (connections, distance, status))
    results+=original
    return results.toArray
  }

  /**
    * For a given superhero (key), this reducer will take both values and keep only the smaller distance + the darkest color
    * @param node1
    * @param node2
    * @return
    */
  def bfsReduce (node1: BFSData, node2:BFSData):BFSData ={
    var edges:Array[Int] = Array()
    var status = UNEXPLORED
    var distance = 9999

    // If one of them is the original node will have some connections, so we preserve them
    if (node1._1.length > 0){
      edges = node1._1
    }
    if (node2._1.length > 0){
      edges = node2._1
    }

    // Keep the min distance
    distance = math.min(node1._2, node2._2)

    // Keep the darkest colour
    if (node1._3 == EXPLORED || node2._3 == EXPLORED){
      status = EXPLORED
    }else{
      if (node1._3 == EXPLORING || node2._3 == EXPLORING) {
        status = EXPLORING
      }else{
        status = UNEXPLORED
      }
    }

    (edges, distance, status)
  }

  def main (args:Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "DegreesOfSeparation")

    // Initialize the accumulator, set to 0 and make it available to the whole cluster
    hitCounter = Some(sc.longAccumulator("FoundCounter"))

    var fullGraph : RDD[BFSNode] = sc.textFile("src/main/resources/Marvel-graph.txt").map(line2BFS)

    // Do 10 iterations looking for target, surely there's a better way to do it
    var i:Int = 0
    do {
      println(s"Running BFS iteration $i")
      // We map original data to a set of BFSNodes constructed with the right connections, status & distance
      val mapped = fullGraph.flatMap(bfsMap)

      // This action forces the RDD to get evaluated, so it's the only point where the accumulator can be updated
      println("Processing " + mapped.count() + " values")

      // The full graph will be reduced following the maximum of getting only sortest path adn mored advanced status
      fullGraph = mapped.reduceByKey(bfsReduce)
      i+=1
    }while (hitCounter.get.isZero)
    println("Found the target, already found following " + hitCounter.get.value + " paths after " + i + " degrees")
  }

}
