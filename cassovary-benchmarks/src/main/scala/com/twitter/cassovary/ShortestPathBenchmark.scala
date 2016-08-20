package com.twitter.cassovary

import com.twitter.cassovary.algorithms.shortestpath.SingleSourceShortestPath
import com.twitter.cassovary.graph.{DirectedGraph, Node}
import scala.util.Random

class ShortestPathBenchmark(graph: DirectedGraph[Node]) extends OperationBenchmark {

  val nodeList = graph.toList
  val random = new Random(System.currentTimeMillis())
  def randomNodeId() = random.nextInt(nodeList.length)

  def operation(): Unit = {
    val node = nodeList(randomNodeId()).id
    val sp = new SingleSourceShortestPath(graph, node)
    sp.allShortestPaths
  }
}

class AllShortestPathsBenchmark(graph: DirectedGraph[Node]) extends OperationBenchmark {

  def operation(): Unit = {
    graph.par foreach { n => new SingleSourceShortestPath(graph, n.id) allShortestPaths }
  }
}
