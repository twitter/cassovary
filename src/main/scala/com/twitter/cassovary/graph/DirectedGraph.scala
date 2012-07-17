/*
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.twitter.cassovary.graph

import GraphDir._
import scala.util.Random

object StoredGraphDir extends Enumeration {
  type StoredGraphDir = Value
  val BothInOut, OnlyIn, OnlyOut, Mutual, Bipartite = Value
  def isOutDirStored(d: StoredGraphDir) = (d == BothInOut) || (d == OnlyOut) || (d == Mutual)
  def isInDirStored(d: StoredGraphDir) = (d == BothInOut) || (d == OnlyIn) || (d == Mutual)

  def isDirStored(dir: GraphDir, storedGraphDir: StoredGraphDir) = {
    dir match {
      case GraphDir.InDir => StoredGraphDir.isInDirStored(storedGraphDir)
      case GraphDir.OutDir => StoredGraphDir.isOutDirStored(storedGraphDir)
    }
  }
}

/**
 * The entry point into a model of a directed graph.  Users typically query a known starting node
 * and then traverse the graph using methods on that {@code Node}.
 */
trait DirectedGraph extends Graph with Iterable[Node] {
  /**
   * Returns the number of nodes in the graph.
   */
  def nodeCount: Int

  /**
   * Describes whether the stored graph is only in-directions, out-directions or both
   */
  val storedGraphDir: StoredGraphDir.StoredGraphDir

  /**
   * Checks if the given graph dir is stored in this graph
   * @param dir the graph dir in question
   * @return if the current graph stores the query graph direction
   */
  def isDirStored(dir: GraphDir) = {
    StoredGraphDir.isDirStored(dir, storedGraphDir)
  }

  /**
   * Returns the total number of directed edges in the graph.  A mutual edge, eg: A -> B and B -> A,
   * counts as 2 edges in this total.
   */
  def edgeCount: Long

  /**
   * the max node id
   */
  lazy val maxNodeId = iterator.foldLeft(0) {
    (mx, node) => mx max node.id
  }

  /**
   * Get a random node that exists in the graph by uniformly sampling from 0 to maxNodeId
   */
  def randomNode:Int = {
    val rand = new Random()
    def getValidNode(nodeId:Int):Int = getNodeById(nodeId) match {
      case Some(n) => nodeId
      case None => getValidNode(rand.nextInt(maxNodeId))
    }
    getValidNode(rand.nextInt(maxNodeId))
  }

  /**
   * Get some random nodes in the graph. Node ids may repeat.
   * @param number the number of random nodes to getAndUpdate
   * @return a list of node ids
   */
  def randomNodes(number:Int):List[Int] = {
    (1 to number).foldLeft(List[Int]()) { (l, _) => randomNode :: l}
  }

  /**
   * Added default toString for debugging (prints max of 10 nodes)
   */
  override def toString = toString(10)

  def toString(numNodes: Int) = {
    "Node count: " + nodeCount + "\n" +
    "Edge count: " + edgeCount + "\n" +
    "Nodes:" +
    iterator.take(numNodes).foldLeft(""){ (accum, node) =>
      accum + ( if (node != null) { "\n" + node } else "")
    }
  }
}
