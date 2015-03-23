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

/**
 * Stores all parameters for Hits algorithm
 * @param maxIterations The maximum number of times that the link analysis algorithm will run before termination
 * @param tolerance The maximum error allowed.
 * @param normalize Flag `true` to return normalized values
 */
case class HitsParams(maxIterations: Option[Int] = Some(100),
                      tolerance: Double = 1.0e-8,
                      normalize: Boolean = true)
  extends Params

/**
 * Stores all values necessary to fully describe one Hits iteration
 * @param hubs Array of values indexed by node id storing hubs values for each node
 * @param authorities Array of values indexed by node id storing authorities values for each node
 * @param error  The T1 error for the current iteration vs the previous iteration.
 */
case class HitsIterationState(hubs: Array[Double],
                         authorities: Array[Double],
                         iteration: Int,
                         error: Double)
  extends IterationState

/**
 * Case class for performing Hits algorithm.  Hits is a link analysis algoritm that returns two values characterizing
 * each node.  Each node receives both a hub value and an authority value.  A node that is characterized by a large
 * hub value is one that has many high quality outbound connections to other nodes, while a node that is characterized
 * by a large authority value has inbound connections from high quality hub nodes.
 * @param params The set of all parameters passed into our algorithm
 */
class Hits(graph: DirectedGraph[Node], params: HitsParams)
  extends LinkAnalysis[HitsIterationState](graph, params, "hits") {
  
  private val normalize = params.normalize

  private def scale(m: Array[Double], byMax: Boolean): Array[Double] = {
    val d = if (byMax) m.max else m.sum
    if (d <= 0) m else m.map { v => v / d }
  }

  protected def defaultInitialState: HitsIterationState = {
    val hubs = Array.fill(graph.maxNodeId + 1)(1.0 / graph.nodeCount)
    val authorities = new Array[Double](graph.maxNodeId + 1)
    new HitsIterationState(hubs, authorities, 0, 100 + tolerance)
  }
  
  def iterate(prevIteration: HitsIterationState): HitsIterationState = {
    val beforeHubs = prevIteration.hubs

    val partialAuth, partialHubs = new Array[Double](graph.maxNodeId + 1)
    graph foreach { node =>
      node.outboundNodes() foreach { nbr => partialAuth(nbr) += beforeHubs(node.id) }
    }
    graph foreach { node =>
      partialHubs(node.id) = node.outboundNodes().foldLeft(0.0) { (partialSum, nbr) => partialSum + partialAuth(nbr) }
    }

    val (afterHubs, afterAuth) = (scale(partialHubs, byMax = true), scale(partialAuth, byMax = true))
    new HitsIterationState(afterHubs, afterAuth, prevIteration.iteration + 1, deltaOfArrays(beforeHubs, afterHubs))
  }

  override def postRun(finalState: HitsIterationState): HitsIterationState = {
    if (normalize)
      new HitsIterationState(scale(finalState.hubs, byMax = false), scale(finalState.authorities, byMax = false), finalState.iteration, finalState.error)
    else
      super.postRun(finalState)
  }
}
