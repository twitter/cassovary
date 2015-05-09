/*
 * Copyright 2015 Twitter, Inc.
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
package com.twitter.cassovary.algorithms.similarity

import com.twitter.cassovary.graph.{GraphDir, DirectedGraph, Node, StoredGraphDir}
import com.twitter.cassovary.graph.GraphDir.GraphDir
import com.twitter.cassovary.util.SmallBoundedPriorityQueue

case class SimilarNodes(val nodeId: Int, val score: Double) extends Ordered[SimilarNodes] {
  import scala.math.Ordered.orderingToOrdered

  def compare(that: SimilarNodes) = (this.score, that.nodeId) compare (that.score, this.nodeId)
}

/**
 * Similarity algorithms are used to analyse the similarity between
 * different nodes of a graph.
 */
trait Similarity {

  def graph: DirectedGraph[Node]

  protected def getNeighbors(dir: GraphDir, nodeId: Int): Option[Set[Int]] = {
    val node = graph.getNodeById(nodeId)
    if (node.isDefined) Some(node.get.neighborIds(dir).toSet)
    else None
  }

  def calculateSimilarity(dir: GraphDir, u: Int, v: Int): Double

  def getTopKSimilarNodes(dir: GraphDir, u: Int, k: Int): Seq[(Int, Double)] = {
    val similarNodesQueue = new SmallBoundedPriorityQueue[SimilarNodes](k)
    val graphNodes = {
      if (graph.storedGraphDir == StoredGraphDir.BothInOut) {
        graph.getNodeById(u).get.neighborIds(dir).toSet[Int].flatMap {
          v => graph.getNodeById(v).get.neighborIds(GraphDir.reverse(dir))
        }.toSet[Int]
      } else {
        graph.toSeq map (node => node.id)
      }
    }
    graphNodes foreach { node =>
      if (node != u) {
        val similarityScore = calculateSimilarity(dir, u, node)
        if (similarityScore > 0.0) similarNodesQueue += SimilarNodes(node, similarityScore)
      }
    }
    similarNodesQueue.top(k).map(node => (node.nodeId, node.score))
  }

  def getTopKAllSimilarPairs(dir: GraphDir, k: Int): Map[Int, Seq[(Int, Double)]] = {
    graph.foldLeft(Map.empty[Int, Seq[(Int, Double)]]) { (partialMap, node) =>
      partialMap + (node.id -> getTopKSimilarNodes(dir, node.id, k))
    }
  }
}
