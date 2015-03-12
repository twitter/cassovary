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

import com.twitter.cassovary.graph.{StoredGraphDir, Node, DirectedGraph}
import com.twitter.cassovary.util.Progress
import com.twitter.logging.Logger

import scala.annotation.tailrec

/**
 * The base class for all parameters fed to our iterative algorithms.
 */
abstract class Params { def maxIterations: Option[Int]; def tolerance: Double }

/**
 * The base class for all iterations through our iterative algorithms.  These classes will simply hold all of the
 * information needed to assess the number of iterations, the error, and the current set of values.
 */
abstract class IterationState { def error: Double; def iteration: Int }

/**
 * `AbstractLinkAnalysis` is the base class that extends `LinkAnalysis`.  All algorithms should inherit from
 * this class.  Let it be noted that we exploit scala streams to accomplish the recursion necessary for
 * our algorithms.  We will not be storing the results every step of the way--instead we find the iteration
 * that first satisfies the condition of exceeding maximum iterations or falls below the error threshold.
 * @param params The set of all parameters passed into our algorithm
 * @param modelName This must be provided by all subclasses for logging purposes.  This should not be
 *                  left up to the user to define.  This must be provided by the implementer.
 * @tparam T `LinkAnalysis` must be generically typed by `Iteration` or one of its subclasses.  An `Iteration`
 *           holds all of the pertinent information for a given algorithm.
 */
abstract class AbstractLinkAnalysis[T <: IterationState](graph: DirectedGraph[Node],
    params: Params, modelName: String) {

  protected val log = Logger.get(modelName)

  protected val maxIterations = params.maxIterations
  protected val tolerance     = params.tolerance

  /**
   * Run a single iteration through our algorithm.
   * @param iteration The starting iteration that our algorithm will be applied to.
   * @return A new iteration.
   */
  def iterate(iteration: T): T

  /**
   * Provides default initial start values for our algorithms.
   * @return An default starting iteration.
   */
  protected def defaultInitialIteration: T

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
  def run(init: T = defaultInitialIteration): T = {
    var iters = init.iteration
    var error = init.error
    var currentIteration = init

    // Let the user know if they can save memory!
    if (graph.maxNodeId.toDouble / graph.nodeCount > 1.1 && graph.maxNodeId - graph.nodeCount > 1000000)
      log.info("Warning - you may be able to reduce the memory usage by renumbering this graph!")

    log.debug(s"Initializing starting ${modelName}...")
    val progress = Progress(s"${modelName}_init", 65536, Some(graph.nodeCount))

    def terminationCondition: Boolean = if (maxIterations.isDefined)
      iters < maxIterations.get && error > tolerance
    else error > tolerance

    while (terminationCondition) {
      val s = iterate(currentIteration)

      log.debug("Finished %sth iteration".format(s.iteration))
      progress.inc

      currentIteration = s
      iters = s.iteration
      error = s.error
    }
    currentIteration
  }
}
