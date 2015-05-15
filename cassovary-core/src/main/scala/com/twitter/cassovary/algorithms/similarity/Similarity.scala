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

import com.twitter.cassovary.graph.{DirectedGraph, GraphDir, Node, StoredGraphDir}
import com.twitter.cassovary.graph.GraphDir.GraphDir
import com.twitter.cassovary.util.SmallBoundedPriorityQueue

case class SimilarNodes(nodeId: Int, score: Double) extends Ordered[SimilarNodes] {
  import scala.math.Ordered.orderingToOrdered

  def compare(that: SimilarNodes) = (this.score, that.nodeId) compare (that.score, this.nodeId)
}

/**
 * Similarity algorithms are used to analyse the similarity between
 * different nodes of a graph.
 */
trait Similarity {

  def graph: DirectedGraph[Node]

  protected def getNeighbors(nodeId: Int, dir: GraphDir = GraphDir.OutDir): Option[Set[Int]] = {
    graph.getNodeById(nodeId).map(_.neighborIds(dir).toSet[Int])
  }

  /**
   * Execute the Similarity algorithm between two nodes.
   * @param u    current node ID
   * @param v    another node ID to check the similarity with current node
   * @param dir  direction of edges in Directed Graph
   * @return     Similarity score of two nodes
   */
  def calculateSimilarity(u: Int, v: Int, dir: GraphDir = GraphDir.OutDir): Double

  /**
   * Iterate over graph nodes and calculate similarity scores for each node. If the graph stores edges in
   * both in and out direction, then iterate over the neighbors in reverse `dir` of neighbors of node `u`.
   * @param u    current node ID
   * @param k    limit for similar nodes
   * @param dir  direction of edges in Directed Graph
   * @return     Seq of top `k` similar node ids and their similarity score with node `u`.
   *             Nodes with non-zero score are added. So, the length of the Seq can be less than `k`
   */
  def getTopKSimilarNodes(u: Int, k: Int, dir: GraphDir = GraphDir.OutDir): Seq[(Int, Double)] = {
    val similarNodesQueue = new SmallBoundedPriorityQueue[SimilarNodes](k)
    val graphNodes = {
      if (graph.storedGraphDir == StoredGraphDir.BothInOut) {
        getNeighbors(u, dir) match {
          case Some(neighbors) => neighbors.flatMap(getNeighbors(_, GraphDir.reverse(dir)).getOrElse(Seq.empty))
          case None => Seq.empty[Int]
        }
      } else {
        graph map (node => node.id)
      }
    }
    graphNodes foreach { node =>
      if (node != u) {
        val similarityScore = calculateSimilarity(u, node, dir)
        if (similarityScore > 0.0) similarNodesQueue += SimilarNodes(node, similarityScore)
      }
    }
    similarNodesQueue.top(k).map(node => (node.nodeId, node.score))
  }

  /**
   * Iterate over each node in the graph and get top `k` nodes with non-zero similarity
   * score for each node.
   * @param k    limit for similar nodes
   * @param dir  direction of edges in Directed Graph
   * @return     Map with key as node Id and value as Seq of top `k` similar node ids and
   *             similarity score.
   */
  def getTopKAllSimilarPairs(k: Int, dir: GraphDir = GraphDir.OutDir): Map[Int, Seq[(Int, Double)]] = {
    graph.foldLeft(Map.empty[Int, Seq[(Int, Double)]]) { (partialMap, node) =>
      partialMap + (node.id -> getTopKSimilarNodes(node.id, k, dir))
    }
  }
}
