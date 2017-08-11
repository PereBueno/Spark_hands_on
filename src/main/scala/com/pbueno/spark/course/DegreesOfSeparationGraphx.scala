package com.pbueno.spark.course

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}

import scala.collection.mutable.ListBuffer

/**
  * Created by POBO on 11/08/2017.
  */
object DegreesOfSeparationGraphx {

  // Function to extract the vertexId (heroId), hero name tuple
  // Input format is 'id "Name"'
  def parseNames(line: String):Option[(VertexId, String)] = {
    val fields = line.split('\"')
    if (fields.length > 1){
      if (fields(0).trim().toLong < 6487 ){ //values above are dummy
        return Some(fields(0).trim().toLong , fields(1))
      }
    }
    // In case no valid data return None, flatmap will ignore Nones and extract values from Somes
    return None
  }

  // Function to build the edges from graph file, graph file format is 'heroId [heroId1 heroId2 ...]'
  def buildEdges (line: String) : List[Edge[Long]] = {
    var edges = new ListBuffer[Edge[Long]]()
    val fields = line.split(" ")
    for (i <- 1 to (fields.length - 1)){
      // Last parameter is 0 in this case, but would contain some value for weight graphs (e.g.: distances)
      edges+= new Edge[Long](fields(0).toLong, fields(i).toLong, 0)
    }
    return edges.toList
  }

  def main (args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "GraphX superheroe degrees of separation")

    val names = sc.textFile("src/main/resources/Marvel-names.txt")
    val nodes = names.flatMap(parseNames)

    val relations = sc.textFile("src/main/resources/Marvel-graph.txt")
    val edges= relations.flatMap(buildEdges)

    // We build our graph and cache it, as we're going to use it several times
    val graph = Graph(nodes, edges, "Default").cache()

    // And party starts
    println("\nTop 10 most connected SuperHeroes:")
    // Merge hero names with the output via join (id, name) and sorts by number of connections
    graph.degrees.join(nodes).sortBy(_._2._1, false).take(10).foreach(println)

    println("\nDegrees of separation for Spidey")
    val root:VertexId = 5306 // that's Spidey

    // We initialize the graph with initial values, 0 for spidey, infinity for the rest
    val initialGraph = graph.mapVertices((id, hero) => if (id == root) 0.0 else Double.PositiveInfinity )

    // And now pregel does it's magic, implementing the Breadth-first search algorithm
    // Initial value and number of iterations indicated
    val bfs = initialGraph.pregel(Double.PositiveInfinity, 10)(
      // It needs 3 functions
      // First one is the incoming value operation, we preserve minimum one
      (id, attr, msg) => math.min(attr, msg),

      // Propagate logic, we send distance +1 to all the neighbours
      triplet => {
        if (triplet.srcAttr != Double.PositiveInfinity){
          Iterator((triplet.dstId, triplet.srcAttr+1))
        }else{
          Iterator.empty
        }
      },

      // Reduce operation, preserving minimum
      (a,b) => math.min(a, b))
      .cache()

    // Printing 100 top results
    bfs.vertices.join(nodes).take(100).foreach(println)

    // Finding degrees from spidey to Devos, the devastator
    bfs.vertices.filter(x => x._1 == 1486).collect().foreach(println)
  }
}
