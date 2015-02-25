package com.twitter.cassovary.algorithms.linkanalysis

import com.twitter.cassovary.graph.{Node, DirectedGraph}

/**
 * Stores all parameters for Hits algorithm
 * @param maxIterations The maximum number of times that the link analysis algorithm will run before termination
 * @param tolerance The maximum error allowed.
 * @param normalize Flag `true` to return normalized values
 */
case class HitsParams(override val maxIterations: Option[Int] = Some(100),
                      override val tolerance: Double = 1.0e-8,
                      normalize: Boolean = true)
  extends Params(maxIterations, tolerance)

/**
 * Stores all values necessary to fully describe one Hits iteration
 * @param hubs Array of values indexed by node id storing hubs values for each node
 * @param authorities Array of values indexed by node id storing authorities values for each node
 * @param error  The T1 error for the current iteration vs the previous iteration.
 */
case class HitsIteration(hubs: Array[Double],
                         authorities: Array[Double],
                         override val iteration: Int,
                         override val error: Double)
  extends Iteration(error, iteration)

/**
 * Case class for performing Hits algorithm.  Hits is a link analysis algoritm that returns two values characterizing
 * each node.  Each node receives both a hub value and an authority value.  A node that is characterized by a large
 * hub value is one that has many high quality outbound connections to other nodes, while a node that is characterized
 * by a large authority value has inbound connections from high quality hub nodes.
 * @param params The set of all parameters passed into our algorithm
 */
case class Hits(graph: DirectedGraph[Node], params: HitsParams)
  extends AbstractLinkAnalysis[HitsIteration](graph, params, "hits") {
  
  val normalize = params.normalize

  private def scale(m: Array[Double], byMax: Boolean): Array[Double] = {
    val d = if (byMax) m.max else m.sum
    if (d <= 0) m else m.map { v => v / d }
  }

  protected def start: HitsIteration = {
    val hubs = new Array[Double](graph.maxNodeId + 1)
    val authorities = new Array[Double](graph.maxNodeId + 1)
    
    graph foreach { n => hubs(n.id) = 1.0 / graph.nodeCount }
    new HitsIteration(hubs, authorities, 0, 100 + tolerance)
  }
  
  def iterate(prevIteration: HitsIteration): HitsIteration = {
    val (beforeHubs, beforeAuth) = (prevIteration.hubs, prevIteration.authorities)

    val rawAuth = beforeAuth.zipWithIndex.foldLeft(new Array[Double](graph.maxNodeId + 1)) { case (partialAuth, (_, node)) =>
      if (graph.existsNodeId(node)) {
        graph.getNodeById(node).get.outboundNodes().foreach { nbr => partialAuth(nbr) += beforeHubs(node)}
      }
      partialAuth
    }
    val rawHubs = beforeHubs.zipWithIndex.map { case (_, node) =>
      if(graph.existsNodeId(node))
        graph.getNodeById(node).get.outboundNodes().foldLeft(0.0) { (partialSum, nbr) => partialSum + rawAuth(nbr) }
      else 0.0
    }

    val (afterHubs, afterAuth) = (scale(rawHubs, byMax = true), scale(rawAuth, byMax = true))

    new HitsIteration(afterHubs, afterAuth, prevIteration.iteration + 1, error(beforeHubs, afterHubs))
  }
  val (hubs, authorities) = if (normalize)
    (scale(result.hubs, byMax = false), scale(result.authorities, byMax = false))
  else (result.hubs, result.authorities)
}
