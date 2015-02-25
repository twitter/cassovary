package com.twitter.cassovary.algorithms.linkanalysis

import com.twitter.cassovary.graph.{Node, DirectedGraph}
import com.twitter.cassovary.util.Progress
import com.twitter.logging.Logger

/**
 * Base trait for link analysis algorithms.  The goal of these is to iteratively determine the importance
 * of the nodes in the graph by analyzing how each of those nodes is connected to its neighbors.
 * @tparam T `LinkAnalysis` must be generically typed by `Iteration` or one of its subclasses.  An `Iteration`
 *          holds all of the pertinent information for a given algorithm.
 */
trait LinkAnalysis[T <: Iteration] extends (Option[T] => T) {

  /**
   * Run the algorithm using an optionally provided `init` argument or with the the default argument provided by start
   * @param init An initial iteration
   */
  def apply(init: Option[T] = None): T = if (init.isDefined) run(init.get) else run()

  /**
   * Run a single iteration through our algorithm.
   * @param iteration The starting iteration that our algorithm will be applied to.
   * @return A new iteration.
   */
  def iterate(iteration: T): T

  /**
   * Run the algorithm.
   */
  def run(initial: T = start): T

  /**
   * Provides default initial start values for our algorithms.
   * @return An default starting iteration.
   */
  protected def start: T

  /**
   * Calculate the error between two arrays using either the T1 error or the T2 error
   * @param t1 Flag `true` to calculate the T1 error (the sum of absolute differences between two arrays).  Flag
   *           `false` to calculate the T2 error (the sum of the squared differences between two arrays).
   */
  protected def error(a: Array[Double], b: Array[Double], t1: Boolean = true): Double = {
    val rawError = (a zip b).map { case (v1,v2) => if(t1) Math.abs(v1 - v2) else Math.pow(v1 - v2, 2.0) }.sum
    if (t1) rawError else Math.sqrt(rawError)
  }
}

/**
 * The base class for all parameters fed to our iterative algorithms.
 * @param maxIterations The maximum number of times that the link analysis algorithm will run before termination
 * @param tolerance The maximum error allowed.
 */
abstract class Params(val maxIterations: Option[Int], val tolerance: Double)

/**
 * The base class for all iterations through our iterative algorithms.  These classes will simply hold all of the
 * information needed to assess the number of iterations, the error, and the current set of values.
 * @param error  The T1 error for the current iteration vs the previous iteration.
 */
abstract class Iteration(val error: Double, val iteration: Int)

/**
 * `AbstractLinkAnalysis` is the base class that extends `LinkAnalysis`.  All algorithms should inherit from
 * this class.  Let it be noted that we exploit scala streams to accomplish the recursion necessary for
 * our algorithms.  We will not be storing the results every step of the way--instead we find the iteration
 * that first satisfies the condition of exceeding maximum iterations or falls below the error threshold.
 * @param params The set of all parameters passed into our algorithm
 * @param modelName This must be provided by all subclasses for logging purposes.  This should not be
 *                  left up to the user to define.  This must be provided by the implementer.
 * @param initial  The starting iteration for our algorithms.  This can be provided by the user or can be
 *                 given the default value provided by the `start` method.
 * @tparam T `LinkAnalysis` must be generically typed by `Iteration` or one of its subclasses.  An `Iteration`
 *           holds all of the pertinent information for a given algorithm.
 */
abstract class AbstractLinkAnalysis[T <: Iteration](graph: DirectedGraph[Node],
    params: Params, modelName: String, initial: Option[T] = None) extends LinkAnalysis[T] {

  protected val log = Logger.get(modelName)

  val maxIterations = params.maxIterations
  val tolerance     = params.tolerance

  /**
   * This is the final `Iteration` object from our stream.  The user can feed this iteration back to `iterate`
   * or `run` if they wish to converge further.
   */
  val result = if ((maxIterations.isDefined && maxIterations.get > 0) || !maxIterations.isDefined)
    this(initial) else start

  /**
   * The final error associated with the initial run.
   */
  val error = result.error

  /**
   * The final iteration number our algorithm terminated at
   */
  val iterations = result.iteration

  def run(init: T = start): T = {
    lazy val stream: Stream[T] = init #:: stream map { h => iterate(h)}

    // Let the user know if they can save memory!
    if (graph.maxNodeId.toDouble / graph.nodeCount > 1.1 && graph.maxNodeId - graph.nodeCount > 1000000)
      log.info("Warning - you may be able to reduce the memory usage by renumbering this graph!")

    log.debug(s"Initializing starting ${modelName}...")
    val progress = Progress(s"${modelName}_init", 65536, Some(graph.nodeCount))

    stream.find { s =>
      log.debug("Beginning %sth iteration".format(s.iteration))
      progress.inc
      if (maxIterations.isDefined)
        s.iteration >= maxIterations.get || s.error <= tolerance
      else
        s.error <= tolerance
    }.get
  }
}
