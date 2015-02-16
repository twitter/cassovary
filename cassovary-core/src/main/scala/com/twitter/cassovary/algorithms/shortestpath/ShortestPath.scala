package com.twitter.cassovary.algorithms.shortestpath

import com.twitter.cassovary.graph.{Node, DirectedGraph}

trait ShortestPath {
  type Path  = Seq[Int]
  type Paths = Seq[Seq[Int]]
  type Stack = List[Seq[Int]]

  def graph: DirectedGraph[Node]

  def shortestPaths(n: Int): Paths

  def allShortestPaths: Map[Int, Paths] = graph.toList.map { n => n.id -> shortestPaths(n.id) }.toMap
}
