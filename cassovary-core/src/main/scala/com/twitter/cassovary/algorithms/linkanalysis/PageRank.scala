package com.twitter.cassovary.algorithms.linkanalysis

import com.twitter.cassovary.graph.{GraphDir, Node, DirectedGraph}
import com.twitter.cassovary.util.Progress

/**
 * Parameters for PageRank
 * @param dampingFactor Probability of NOT randomly jumping to another node
 * @param maxIterations The maximum number of times that the link analysis algorithm will run before termination
 * @param tolerance The maximum error allowed.
 */
case class PageRankParams(dampingFactor: Double = 0.85,
                          override val maxIterations: Option[Int] = Some(10),
                          override val tolerance: Double = 1e-8)
  extends Params(maxIterations, tolerance)

/**
 * An object containing information to fully describe a page rank iteration.
 * @param pageRank The current set of pageRank values
 * @param error  The T1 error for the current iteration vs the previous iteration.
 */
case class PageRankIteration(pageRank: Array[Double],
                             override val error: Double,
                             override val iteration: Int)
  extends Iteration(error, iteration)

/**
 * PageRank is a link analysis algorithm designed to measure the importance of nodes in a graph.
 * Popularized by Google.
 *
 * Unoptimized for now, and runs in a single thread.
 */
case class PageRank(graph: DirectedGraph[Node], params: PageRankParams)
  extends AbstractLinkAnalysis[PageRankIteration](graph, params, "pagerank") {

  lazy val dampingFactor = params.dampingFactor
  lazy val dampingAmount = (1.0D - dampingFactor) / graph.nodeCount

  protected def start: PageRankIteration = {
    val initial = new Array[Double](graph.maxNodeId + 1)
    graph foreach { n => initial(n.id) = 1.0 / graph.nodeCount }
    PageRankIteration(initial, 100 + tolerance, 0)
  }

  def iterate(prevIteration: PageRankIteration): PageRankIteration = {
    val beforePR = prevIteration.pageRank
    val afterPR  = new Array[Double](graph.maxNodeId + 1)

    log.debug("Calculating new PageRank values based on previous iteration...")
    val prog = Progress("pagerank_calc", 65536, Some(graph.nodeCount))
    graph.foreach { node =>
      val givenPageRank = beforePR(node.id) / node.neighborCount(GraphDir.OutDir)
      node.neighborIds(GraphDir.OutDir).foreach { neighborId =>
        afterPR(neighborId) += givenPageRank
      }
      prog.inc
    }

    log.debug("Damping...")
    val progress_damp = Progress("pagerank_damp", 65536, Some(graph.nodeCount))
    if (dampingAmount > 0) {
      graph.foreach { node =>
        afterPR(node.id) = dampingAmount + dampingFactor * afterPR(node.id)
        progress_damp.inc
      }
    }
    PageRankIteration(afterPR, error(prevIteration.pageRank, afterPR), prevIteration.iteration + 1)
  }
  val pageRank = result.pageRank
}
