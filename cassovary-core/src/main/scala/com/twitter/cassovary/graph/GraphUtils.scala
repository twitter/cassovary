/*
 * Copyright 2014 Twitter, Inc.
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
import com.twitter.finagle.stats.{DefaultStatsReceiver, Stat}
import it.unimi.dsi.fastutil.ints.Int2IntMap
import it.unimi.dsi.fastutil.objects.Object2IntMap
import scala.util.Random


/**
 * This class contains some common graph utilities and convenience functions.
 */
class GraphUtils[+V <: Node](val graph: Graph[V]) {

  private val statsReceiver = DefaultStatsReceiver

  import GraphUtils._

  /**
   * This is a breadth-first all paths walk with possible multiple visits to a node
   * along the direction specified by `dir`.
   *
   * All paths walk is a queue based walk, where each node adds all her neighbors to the queue
   * and each node can be visited multiple times.
   * @param startNodeId(s) node(s) to start the random walk from. These must exist in the graph.
   * @param numTopPathsPerNode number of paths per node to keep track of
   * @param walkLimits walk limits
   * @return a pair of elements
   *         1. Counter tracking a visited node's id V and the number of visits to that node.
   *         2. Counter tracking a visited node's id V and a set of neighbors. The neighbors
   *         are sorted in decreasing order by occurrence.*/
  def allPathsWalk(dir: GraphDir, startNodeId: Int, numTopPathsPerNode: Int,
                   walkLimits: Walk.Limits):
      (VisitsCounter, PrevNbrCounter) = {

    val visitsCounter = new VisitsCounter
    val prevNbrCounter = new PrevNbrCounter(Some(numTopPathsPerNode), false)

    if (graph.existsNodeId(startNodeId)) {
      val traversedNodes = new BreadthFirstTraverser(graph, dir, Seq(startNodeId),
        walkLimits, Some(prevNbrCounter))

      statsReceiver.counter("bfs_walk_request").incr()
      Stat.time(statsReceiver.stat("bfs_walk_traverse")) {
        traversedNodes.foreach { node =>
          // prevNbrCounter is mutated within BreadthFirstTraverser
          visitsCounter.visit(node)
        }
      }
    }

    (visitsCounter, prevNbrCounter)
  }

  /**
   * This is a breadth-first walk along the direction specified by `dir`.
   * @param startNodeId node to start the random walk from. These must exist in the graph.
   * @param walkLimits walk limits
   * @return a sequence of visited nodes and depths of the visit (or empty sequence if
   *         startNodeId was not found in the graph)
   */
  def bfsWalk(startNodeId: Int, dir: GraphDir, walkLimits: Walk.Limits):
    Seq[(Int, Int)] = {

    val prevNbrCounter = new PrevNbrCounter(Some(1), false)

    if (graph.existsNodeId(startNodeId)) {
      val traversedNodes = new BreadthFirstTraverser(graph, dir, Seq(startNodeId),
        walkLimits, Some(prevNbrCounter))

      statsReceiver.counter("bfs_walk_request").incr()
      Stat.time(statsReceiver.stat ("bfs_walk_traverse")) {
        traversedNodes.map { node =>
          (node.id, traversedNodes.depth(node.id).get)
        }.toSeq
      }
    } else Seq()
  }

  /**
   * Do a random walk starting from the set of nodes with ids `startNodeIds`.
   * The walk maintains a count of the number of times that nodes have been visited
   * during the walk.
   * @param startNodeIds nodes to start the random walk from
   * @param walkParams the `RandomWalkParams` random walk parameters
   * @return a tuple of two elements.
   *         The first is a counter tracking a visited node's id to the number of visits to that node.
   *         The second is a counter tracking a visited node's id to the paths visited while hitting
   *         that node. The paths are sorted in decreasing order by occurrence
   *         Each path is kept as a `DirectedPath`.
   */
  def randomWalk(dir: GraphDir, startNodeIds: Seq[Int], walkParams: RandomWalkParams)():
      (VisitsCounter, Option[PathsCounter]) = {
    val startNodesExist = (startNodeIds.length > 0) && startNodeIds.forall(graph.existsNodeId)

    val visitsCounter = new VisitsCounter
    val pathsCounterOption = walkParams.numTopPathsPerNode match {
      case Some(k) if k > 0 => Some(new PathsCounter(k, startNodeIds))
      case _ => None
    }

    if (startNodesExist) {
      val traversedNodes = new RandomBoundedTraverser(graph, dir, startNodeIds,
        walkParams.numSteps, walkParams)

      Stat.time(statsReceiver.stat("random_walk_traverse")) {
        traversedNodes.foreach { node =>
          visitsCounter.visit(node)
          if (pathsCounterOption.isDefined) {
            pathsCounterOption.get.visit(node)
          }
        }
      }
    }
    (visitsCounter, pathsCounterOption)
  }

  /**
   * Calculates the reputation of graph nodes personalized to a given node based on a random walk.
   * @param startNodeIds the ids of the node to get personalized reputations for
   * @param walkParams the `RandomWalkParams` random walk parameters
   * @return a 2-tuple:
   *         1. List of (node's id, the number of visits made to the node) sorted in decreasing
   *            order of the number of visits, and
   *         2. A mapping, for a visited node with id V to the top paths leading to V
   *            in the form of (P as a `DirectedPath`, frequency of walking P).
   */
  def calculatePersonalizedReputation(startNodeIds: Seq[Int], walkParams: RandomWalkParams):
      (collection.Map[Int, Int], Option[collection.Map[Int, Object2IntMap[DirectedPath]]]) = {
    Stat.time(statsReceiver.stat("%s_total".format("PTC"))) {
      val (visitsCounter, pathsCounterOption) = randomWalk(walkParams.dir, startNodeIds, walkParams)
      val topPathsOption = pathsCounterOption flatMap { counter => Some(counter.infoAllNodes) }
      (visitsCounter.infoAllNodes, topPathsOption)
    }
  }

  def calculatePersonalizedReputation(startNodeId: Int, walkParams: RandomWalkParams):
      (collection.Map[Int, Int], Option[collection.Map[Int, Object2IntMap[DirectedPath]]]) = {
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
  def calculateAllPathsWalk(startNodeId: Int, dir: GraphDir, numTopPathsPerNode: Int,
                            walkLimits: Walk.Limits):
      (collection.Map[Int, Int], collection.Map[Int, Int2IntMap]) = {
    Stat.time(statsReceiver.stat("%s_total".format("AllPathsWalk"))) {
      val (visitsCounter, prevNbrCounter) = allPathsWalk(dir, startNodeId,
        numTopPathsPerNode, walkLimits)
      (visitsCounter.infoAllNodes, prevNbrCounter.infoAllNodes)
    }
  }

  /**
   * @param id the id of the node to count neighbors of
   * @param dir the direction of interest
   * @return number of neighbors (i.e., number of nodes 1 hop away) in `dir`.
   *         Warning: Lossy: Returns 0 if id is not found.
   */
  def neighborCount(id: Int, dir: GraphDir) = graph.getNodeById(id).map(_.neighborCount((dir))).getOrElse(0)


  /**
   * Assuming that the node stores both directions of edges, calculate the number of mutual edges
   * incident on this node
   * @return number of neighbors of node that have an edge to and from this node
   */
  def getNumMutualEdgesBothDirs(node: Node): Long = {
    val (dirSmall, dirLarge) = if (node.outboundCount < node.inboundCount)
      (node.outboundNodes(), node.inboundNodes())
    else
      (node.inboundNodes(), node.outboundNodes())
    val setSmall = dirSmall.toSet
    val numEdges = dirLarge.foldLeft(0L) { (num, curr) =>
      if (setSmall contains curr) num+1
      else num
    }
    numEdges
  }
}

object GraphUtils {

  /**
   * Parameters of a walk (such as a random walk, or a breadth first walk).
   * @param numSteps number of steps to take in the walk
   * @param resetProbability the probability with which to reset back to `startNodeId`.
   *        Must lie between 0.0 and 1.0, both inclusive. Ignored for non-random walks
   * @param maxNumEdgesThresh Max number of edges allowed for a node
   *        beyond which the next random step is `startNodeId` regardless of anything else
   * @param numTopPathsPerNode the number of top paths to `node` to maintain, None if we don't
   *        want to maintain them at all
   * @param maxDepth the maximum depth to visit in depth-related search (e.g. startNodeId has
   *        depth 0, its immediate neighbors have depth 1, etc.). None if we don't want to maintain
   *        them at all
   * @param visitSameNodeOnce if true, the walk would only visit the same node once
   *        and will likely restart to a start node when visiting the same node twice
   * @param dir traverse out-direction or in-direction
   * @param stable if true, use a fixed random number generator in the random walk
   * @param filterHomeNodeByNumEdges if true, home node will be checked for maxNumEdgesThresh
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
