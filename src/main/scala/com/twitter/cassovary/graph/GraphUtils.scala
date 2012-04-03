/*
 * Copyright 2012 Twitter, Inc.
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
package com.twitter.cassovary.graph

import com.twitter.cassovary.graph.GraphDir._
import com.twitter.cassovary.graph.tourist._
import com.twitter.ostrich.stats.Stats

import it.unimi.dsi.fastutil.ints.{Int2IntMap, Int2ObjectMap}
import net.lag.logging.Logger
import scala.util.Random

/**
 * This class contains some common graph utilities and convenience functions.
 */

class GraphUtils(val graph: Graph) {
  import GraphUtils._

  private val log = Logger.get

  /**
   * Do a walk on the {@code nodes} in a graph.
   * @param nodes the nodes to visit in this walk
   * @param tourists each tourist maintains some state and updates that state on visiting a node
   * @return Seq of tourist-specific-returned information
   */

  def walk(nodes: Iterator[Node], walkFunc: (Node => Unit)) = {
    nodes foreach { node =>
      walkFunc(node)
    }
  }

  /**
   * The following are different types of graph traversals (aka "walks").
   */

  /**
   * This is a breadth-first walk along the direction specified by {@code dir}.
   * @param startNodeId(s) node(s) to start the random walk from. These must exist in the graph.
   * @param walkParams the parameters specifying this random walk
   * @return a tuple of two elements
   *         The first is a counter tracking a visited node's id V and the number of visits to that node.
   *         The second is a counter tracking a visited node's id V and a set of neighbors. The neighbors
   *         are sorted in decreasing order by occurrence
   */
  def bfsWalk(dir: GraphDir, startNodeId: Int, walkParams: RandomWalkParams)():
      (VisitsCounter, PrevNbrCounter) = {

    val visitsCounter = new VisitsCounter
    val prevNbrCounter = new PrevNbrCounter(walkParams.numTopPathsPerNode, walkParams.visitSameNodeOnce)

    if (graph.existsNodeId(startNodeId)) {
      val traversedNodes = new BreadthFirstTraverser(graph, dir, Seq(startNodeId),
        walkParams.maxDepth, walkParams.maxNumEdgesThresh, walkParams.numSteps,
        walkParams.visitSameNodeOnce, Some(prevNbrCounter))
      with BoundedIterator[Node] {
        val maxSteps = walkParams.numSteps
      }

      Stats.incr("bfs_walk_request", 1)
      Stats.time ("bfs_walk_traverse") {
        walk(traversedNodes, { node =>
          // prevNbrCounter is mutated within BreadthFirstTraverser
          visitsCounter.visit(node)
        })
      }
    }

    (visitsCounter, prevNbrCounter)
  }

  /**
   * Do a random walk starting from the set of nodes with ids {@code startNodeIds}.
   * The walk maintains a count of the number of times that nodes have been visited
   * during the walk.
   * @param startNodeIds nodes to start the random walk from
   * @param walkParams the {@link RandomWalkParams} random walk parameters
   * @return a tuple of two elements.
   *         The first is a counter tracking a visited node's id to the number of visits to that node.
   *         The second is a counter tracking a visited node's id to the paths visited while hitting
   *         that node. The paths are sorted in decreasing order by occurrence
   *         Each path is kept as a {@link DirectedPath}.
   */
  def randomWalk(dir: GraphDir, startNodeIds: Seq[Int], walkParams: RandomWalkParams)():
      (VisitsCounter, Option[PathsCounter]) = {
    val startNodesExist = (startNodeIds.length > 0) && startNodeIds.foldLeft(true) { (exists, elem) =>
      exists && graph.existsNodeId(elem)
    }

    val visitsCounter = new VisitsCounter
    val pathsCounterOption = walkParams.numTopPathsPerNode match {
      case Some(k) if (k > 0) => Some(new PathsCounter(k, startNodeIds))
      case _ => None
    }

    if (startNodesExist) {
      val traversedNodes = new RandomBoundedTraverser(graph, dir, startNodeIds,
        walkParams.numSteps, walkParams)

      Stats.time("random_walk_traverse") {
        walk(traversedNodes, { node =>
          visitsCounter.visit(node)
          if (pathsCounterOption.isDefined) {
            pathsCounterOption.get.visit(node)
          }
        })
      }
    }
    (visitsCounter, pathsCounterOption)
  }

  /**
   * Calculates the reputation of graph nodes personalized to a given node based on a random walk.
   * @param startNodeIds the ids of the node to get personalized reputations for
   * @param walkParams the {@link RandomWalkParams} random walk parameters
   * @return a 2-tuple:
   *         1. List of (node's id, the number of visits made to the node) sorted in decreasing
   *            order of the number of visits, and
   *         2. A mapping, for a visited node with id V to the top paths leading to V
   *            in the form of (P as a {@link DirectedPath}, frequency of walking P).
   */
  def calculatePersonalizedReputation(startNodeIds: Seq[Int], walkParams: RandomWalkParams):
      (Int2IntMap, Option[Int2ObjectMap[Array[DirectedPath]]]) = {
    Stats.time ("%s_total".format("PTC")) {
      val (visitsCounter, pathsCounterOption) = randomWalk(walkParams.dir, startNodeIds, walkParams)
      val topPathsOption = pathsCounterOption flatMap { counter => Some(counter.infoAllNodes) }
      (visitsCounter.infoAllNodes, topPathsOption)
    }
  }

  def calculatePersonalizedReputation(startNodeId: Int, walkParams: RandomWalkParams):
      (Int2IntMap, Option[Int2ObjectMap[Array[DirectedPath]]]) = {
    calculatePersonalizedReputation(Seq(startNodeId), walkParams)
  }

  /**
   * Does a breadth-first-walk starting from {@code startNodeId} using the walk
   * parameters specified in {@code walkParams}. Returns a 2-tuple:
   * 1. List of (node's id, the number of visits made to the node) sorted in decreasing
   *    order of the number of visits, and
   * 2. A mapping for a visited node with id V to the top paths leading to V
   *    in the form of (P as a {@link DirectedPath}, frequency of walking P).
   */
  def calculateBFS(startNodeId: Int, walkParams: RandomWalkParams):
      (Int2IntMap, Int2ObjectMap[Array[Int]]) = {
    Stats.time("%s_total".format("BFS")) {
      val (visitsCounter, prevNbrCounter) = bfsWalk(walkParams.dir, startNodeId, walkParams)
      (visitsCounter.infoAllNodes, prevNbrCounter.infoAllNodes)
    }
  }

  /**
   * @param id the id of the node to count neighbors of
   * @param dir the direction of interest
   * @return number of neighbors (i.e., number of nodes 1 hop away) in {@code dir}.
   *         Warning: Lossy: Returns 0 if id is not found.
   */
  def neighborCount(id: Int, dir: GraphDir) = funcById(id, dir,
    (nd: Node, dir: GraphDir) => nd.neighborCount(dir), 0)

  // helper to be used in functions that take nodeId and need to log warnings
  // in the case that the id is not found
  private def onNodeIdNotFound(desc: String, id: Int) {
    log.warning("(%s) Node with id = %d not found!", desc, id)
  }

  // convenience wrapper for methods that run on node ids
  private def funcById[T](id: Int, dir: GraphDir, f: (Node, GraphDir) => T): Option[T] = {
    graph.getNodeById(id) match {
      case Some(node) => Some(f(node, dir))
      case None => None
    }
  }

  // convenience wrapper for methods that provide a default value if node is not found
  private def funcById[T](id: Int, dir: GraphDir, f: (Node, GraphDir) => T, defaultVal: T): T = {
    funcById(id, dir, f).getOrElse(defaultVal)
  }

}

object GraphUtils {

  /**
   * Parameters of a walk (such as a random walk, or a breadth first walk).
   * @param numSteps number of steps to take in the walk
   * @param resetProbability the probability with which to reset back to {@code startNodeId}.
   *        Must lie between 0.0 and 1.0, both inclusive. Ignored for non-random walks
   * @param maxNumEdgesThresh Max number of edges allowed for a node
   *        beyond which the next random step is {@code startNodeId} regardless of anything else
   * @param numTopPathsPerNode the number of top paths to {@code node} to maintain, None if we don't
   *        want to maintain them at all
   * @param maxDepth the maximum depth to visit in depth-related search (e.g. startNodeId has
   *        depth 0, its immediate neighbors have depth 1, etc.). None if we don't want to maintain
   *        them at all
   * @param visitSameNodeOnce if true, the walk would only visit the same node once
   *        and will likely restart to a start node when visiting the same node twice
   * @param dir traverse out-direction or in-direction
   * @param stable if true, use a fixed random number generator in the random walk
   * @param filterHOmeNodeByNumEdges if true, home node will be checked for maxNumEdgesThresh
   * when visited during random walk
  */
  case class RandomWalkParams(numSteps: Long,
      resetProbability: Double,
      maxNumEdgesThresh: Option[Int] = None,
      numTopPathsPerNode: Option[Int] = None,
      maxDepth: Option[Int] = None,
      visitSameNodeOnce: Boolean = false,
      dir: GraphDir = GraphDir.OutDir,
      stable: Boolean = false,
      filterHomeNodeByNumEdges: Boolean = false) {
    /**
     * The pruning function that guides the pruning of a random walk. If the random walk
     * reached a node, it resets to the starting node if pruneFn returns true for that node.
     * Can be overridden by subclasses.
     */
    val pruneFn: Node => Boolean = NodeUtils.hasTooManyEdges(OutDir, maxNumEdgesThresh)

    private val seed = 268430371266199L
    lazy val randNumGen = if (stable) new Random(seed) else new Random
    require(resetProbability >= 0 && resetProbability <= 1.0,
      "reset probability must be between 0.0 and 1.0")
  }
}
