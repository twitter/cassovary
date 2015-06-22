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
package com.twitter.cassovary.algorithms.linkanalysis

import com.twitter.cassovary.graph.{DirectedGraph, Node}
import com.twitter.cassovary.util.Progress
import scala.collection.immutable.BitSet

/**
 * Parameters for PageRank
 * @param dampingFactor Probability of NOT randomly jumping to another node
 * @param maxIterations The maximum number of times that the link analysis algorithm will run before termination
 * @param tolerance The maximum error allowed.
 */
case class PageRankParams(dampingFactor: Double = 0.85,
                          maxIterations: Option[Int] = Some(10),
                          tolerance: Double = 1e-8)
  extends Params

/**
 * Class containing information to fully describe a page rank iteration.
 * @param pageRank The current set of pageRank values
 * @param error  The T1 error for the current iteration vs the previous iteration.
 */
case class PageRankIterationState(pageRank: Array[Double], error: Double, iteration: Int)
  extends IterationState

/**
 * PageRank is a link analysis algorithm designed to measure the importance of nodes in a graph.
 * Popularized by Google.
 *
 * Unoptimized for now, and runs in a single thread.
 */
class PageRank(graph: DirectedGraph[Node], params: PageRankParams = PageRankParams())
  extends LinkAnalysis[PageRankIterationState](graph, params, "pagerank") {

  // outboundCount is used to find the number of outgoing connections a graph stored OnlyIn has.
  private lazy val outboundCount = new Array[Double](graph.maxNodeId + 1)
  private val nodeCount = graph.nodeCount

  //We must find nodes that have zero outbound connections to correctly handle these.
  private val zeroOutgoing = if (isInStored) {

    //If the graph is stored OnlyIn, it is most efficient to find which nodes are not dangling and do a set subtraction
    //from there.
    val nonDangling = graph.foldLeft(BitSet()){ (partialSet, node) =>
      val neighbors = efficientNeighbors(node)
      neighbors foreach { nbr => outboundCount(nbr) += 1 }
      partialSet ++ neighbors
    }
    graph.view.map { _.id }.filter { !nonDangling.contains(_) }
  }
  else {
    //If the graph is stored OnlyOut, then the computation is much simpler and does not require complete traversal
    //of the graph twice to find dangling nodes.
    graph.foldLeft(BitSet()){ (partialSet, node) =>
      if (efficientNeighbors(node).isEmpty) partialSet + node.id else partialSet
    }
  }

  lazy val dampingFactor = params.dampingFactor
  lazy val dampingAmount = (1.0 - dampingFactor) / nodeCount

  protected def defaultInitialState: PageRankIterationState = {
    val initial = Array.tabulate(graph.maxNodeId + 1){ n => if (graph.existsNodeId(n)) 1.0 / nodeCount else 0.0 }
    PageRankIterationState(initial, 100 + tolerance, 0)
  }

  def iterate(prevIteration: PageRankIterationState): PageRankIterationState = {
    val beforePR = prevIteration.pageRank
    val afterPR  = new Array[Double](graph.maxNodeId + 1)

    log.debug("Calculating new PageRank values based on previous iteration...")
    val prog = Progress("pagerank_calc", 65536, Some(graph.nodeCount))

    //must correct for nodes with no outgoing connections
    val dangleSum = dampingFactor * zeroOutgoing.foldLeft(0.0){ (partialSum, n) => partialSum + beforePR(n) } / nodeCount

    graph foreach { node =>
      val neighbors = efficientNeighbors(node)
      if (isInStored) {
        afterPR(node.id) = neighbors.foldLeft(0.0) {
          (partialSum, nbr) => partialSum + dampingFactor * beforePR(nbr) / outboundCount(nbr)
        } + dampingAmount + dangleSum
      }
      else {
        neighbors foreach { nbr => afterPR(nbr) += dampingFactor * beforePR(node.id) / node.outboundCount }
        afterPR(node.id) += dangleSum + dampingAmount
      }
      prog.inc
    }
    PageRankIterationState(afterPR, deltaOfArrays(prevIteration.pageRank, afterPR), prevIteration.iteration + 1)
  }
}
