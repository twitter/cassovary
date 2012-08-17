package com.twitter.cassovary.algorithms

import com.twitter.cassovary.graph.{GraphDir, DirectedGraph}
import net.lag.logging.Logger

/**
 * Parameters for PageRank
 * @param dampingFactor Probability of NOT randomly jumping to another node
 * @param iterations How many PageRank iterations do you want?
 */
case class PageRankParams(dampingFactor: Double = 0.85,
                          iterations: Option[Int] = Some(10))

/**
 * PageRank is a link analysis algorithm designed to measure the importance of nodes in a graph.
 * Popularized by Google.
 */
object PageRank {

  /**
   * Execute PageRank.
   * Note that the memory usage of this implementation is
   * proportional to the graph's maxId - you might want to renumber the
   * graph before running PageRank.
   * @param graph A DirectedGraph instance
   * @param params PageRankParams from above
   * @return An array of doubles, with indices corresponding to node ids
   */
  def apply(graph:DirectedGraph, params:PageRankParams): Array[Double] = {
    val pr = new PageRank(graph, params)
    pr.run
  }

  def iterate(graph:DirectedGraph, params:PageRankParams, prArray: Array[Double]) = {
    val pr = new PageRank(graph, params)
    pr.iterate(prArray: Array[Double])
  }
}

private class PageRank(graph:DirectedGraph, params:PageRankParams) {

  private val log = Logger.get("PageRank")

  val dampingFactor = params.dampingFactor
  val dampingAmount = (1.0D - dampingFactor) / graph.nodeCount

  def run: Array[Double] = {

    // Let the user know if they can save memory!
    if (graph.maxNodeId.toDouble / graph.nodeCount > 1.1 && graph.maxNodeId - graph.nodeCount > 1000000)
      log.info("Warning - you may be able to reduce the memory usage of PageRank by renumbering this graph!")

    var beforePR = new Array[Double](graph.maxNodeId + 1)
    log.info("Initializing starting PageRank...")
    var count = 0
    val initialPageRankValue = 1.0D / graph.nodeCount
    graph.foreach { node =>
      beforePR(node.id) = initialPageRankValue
      count += 1
      if (count % 65536 == 0) {
        log.info("Processed %s nodes...".format(count))
      }
    }

    (0 until params.iterations.get).foreach { i =>
      log.info("Beginning %sth iteration".format(i))
      beforePR = iterate(beforePR)
    }

    beforePR
  }

  def iterate(beforePR: Array[Double]): Array[Double] = {
    val afterPR = new Array[Double](graph.maxNodeId + 1)

    log.info("Calculating...")
    var count = 0
    graph.foreach { node =>
      val givenPageRank = beforePR(node.id) / node.neighborCount(GraphDir.OutDir)
      node.neighborIds(GraphDir.OutDir).foreach { neighborId =>
        afterPR(neighborId) += givenPageRank
      }
      count += 1
      if (count % 65536 == 0) {
        log.info("Processed %s nodes...".format(count))
      }
    }

    log.info("Damping...")
    count = 0
    if (dampingAmount > 0) {
      graph.foreach { node =>
        afterPR(node.id) = dampingAmount + dampingFactor * afterPR(node.id)
      }
      count += 1
      if (count % 65536 == 0) {
        log.info("Processed %s nodes...".format(count))
      }
    }
    afterPR
  }

}

