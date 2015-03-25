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

import com.twitter.cassovary.graph.{StoredGraphDir, DirectedGraph, Node}
import com.twitter.cassovary.util.Progress
import com.twitter.logging.Logger

/**
 * The base class for all parameters fed to our iterative algorithms.
 */
abstract class Params {
  def maxIterations: Option[Int]
  def tolerance: Double
}

/**
 * The base class for all iterations through our iterative algorithms.  These classes will simply hold all of the
 * information needed to assess the number of iterations, the error, and the current set of values.
 */
abstract class IterationState {
  def error: Double
  def iteration: Int
}

/**
 * All link analysis algorithms should inherit from the `LinkAnalysis` base class.
 * @param params The set of all parameters passed into our algorithm
 * @param modelName This must be provided by all subclasses for logging purposes.  This should not be
 *                  left up to the user to define.  This must be provided by the implementer.
 * @tparam T `LinkAnalysis` must be generically typed by `IterationState` or one of its subclasses.  An `IterationState`
 *           holds all of the pertinent information for a given algorithm.
 */
abstract class LinkAnalysis[T <: IterationState](graph: DirectedGraph[Node],
                                                 params: Params, modelName: String) {

  protected val log           = Logger.get(modelName)
  protected val maxIterations = params.maxIterations
  protected val tolerance     = params.tolerance
  protected val isInStored    = StoredGraphDir.isInDirStored(graph.storedGraphDir)

  protected def efficientNeighbors(node: Node): Seq[Int] = if (isInStored) node.inboundNodes() else node.outboundNodes()
  protected def efficientNeighborCount(node: Node): Int  = efficientNeighbors(node).size

  /**
   * Run a single iteration through our algorithm.
   * @param currState The starting iteration that our algorithm will be applied to.
   * @return A new iteration.
   */
  def iterate(currState: T): T

  /**
   * Provides default initial start values for our algorithms.
   * @return An default starting iteration.
   */
  protected def defaultInitialState: T

  /**
   * Calculate the error between two arrays using either the T1 error or the T2 error.  This is a
   * convenience method.
   * @param t1 Flag `true` to calculate the T1 error (the sum of absolute differences between two arrays).  Flag
   *           `false` to calculate the T2 error (the sum of the squared differences between two arrays).
   */
  protected def deltaOfArrays(a: Array[Double], b: Array[Double], t1: Boolean = true): Double = {
    val difference = (a zip b).map { case(v1, v2) => if (t1) Math.abs(v1 - v2) else Math.pow(v1 - v2, 2) }.sum
    if (t1) difference else Math.sqrt(difference)
  }

  /**
   * Run the algorithm to completion according to the parameters passed on instantiation.
   * @param init The starting point of the iteration.  If no iteration is given, the default start
   *             is assumed
   * @return The final iteration.
   */
  def run(init: T = defaultInitialState): T = {
    var currentIteration = init


    // Let the user know if they can save memory!
    if (graph.maxNodeId.toDouble / graph.nodeCount > 1.1 && graph.maxNodeId - graph.nodeCount > 1000000)
      log.info("Warning - you may be able to reduce the memory usage by renumbering this graph!")

    log.debug(s"Initializing starting ${modelName}...")
    val progress = Progress(s"${modelName}_init", 65536, Some(graph.nodeCount))

    def terminate(currState: IterationState): Boolean = if (maxIterations.isDefined)
      currState.iteration >= maxIterations.get || currState.error <= tolerance
    else currState.error <= tolerance

    while (!terminate(currentIteration)) {
      val s = iterate(currentIteration)
      log.debug("Finished %sth iteration".format(s.iteration))
      progress.inc
      currentIteration = s
    }
    postRun(currentIteration)
  }

  /**
   * Run final processing of the state.  Unless the method is overridden, it will just return the final state.
   * If normalization needs to happen upon convergence, this method is the ideal location for such
   * @param finalState
   * @return
   */
  def postRun(finalState: T): T = finalState
}
