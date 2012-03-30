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
import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import com.twitter.cassovary.graph.tourist.{InfoKeeper, PrevNbrCounter}
import com.twitter.ostrich.stats.Stats
import it.unimi.dsi.fastutil.ints.IntArrayList
import net.lag.logging.Logger
import scala.collection.mutable
import scala.util.Random

/**
 * A Traverser traverses the graph in a certain order of nodes.
 */
trait Traverser extends Iterator[Node] { self =>
  /**
   * Traverser only visits node Ids listed in some node's edges, and thus
   * we know that node must exist, safe to assume graph.getNodeById return non-None value
   */
  protected def getExistingNodeById(graph: Graph, id: Int) = graph.getNodeById(id).get
}

/**
 * Bounds an iterator to go no more than a specified maximum number of steps
 */
trait BoundedIterator[T] extends {  private var numStepsTaken = 0L } with Iterator[T] {
  val maxSteps: Long

  abstract override def next = {
    numStepsTaken += 1
    super.next
  }

  abstract override def hasNext = ( (numStepsTaken < maxSteps) && super.hasNext)
}

/**
 * Randomly traverse the graph, going from one node to a random neighbor in direction {@code dir}.
 * @param graph the graph to traverse on
 * @param dir direction in which to traverse
 * @param homeNodeIds the ids of nodes that the traverser will go to next if the node has no
 * neighbors, or we reset the traversal with probability {@code resetProbability},
 * or if the number of out-going edges at the current node exceeds {@code maxNumEdgesThresh}
 * @param onlyOnce specifies whether the same node should only be allowed to be visited once
 * in any path.
 * @param randNumGen a random number generator (for stable walk, a seeded random number
 * generator is used).
 */
class RandomTraverser(graph: Graph, dir: GraphDir, homeNodeIds: Seq[Int],
                      resetProbability: Double, maxNumEdgesThresh: Option[Int], onlyOnce: Boolean,
                      randNumGen: Random, maxDepth: Option[Int], filterHomeNodeByNumEdges: Boolean)
    extends Traverser {  self =>

  private var currNode: Node = null
  private var homeNode: Node = null
  private val homeNodeIdSet = Set(homeNodeIds: _*)

  private val seenNodesTracker = new InfoKeeper[Int] {
    override val onlyOnce = self.onlyOnce
  }

  protected def seenBefore(id: Int) = seenNodesTracker.infoOfNode(id).isDefined
  private var pathLength = 0

  private def goHome(): Int = {
    pathLength = 0
    NodeUtils.pickRandNodeId(homeNodeIds, randNumGen)
  }

  private def takeRandomStep(): Int = {
    val nextRandom = randNumGen.nextDouble()
    val needToFilterByNumEdges = (filterHomeNodeByNumEdges || !(homeNodeIdSet contains currNode.id))
    if (nextRandom < resetProbability ||
        (needToFilterByNumEdges && NodeUtils.hasTooManyEdges(dir, maxNumEdgesThresh)(currNode))) {
      goHome()
    } else {
      currNode.randomNeighbor(dir, randNumGen).getOrElse(goHome())
    }
  }

  def next = {
    val nextNodeId = if (currNode == null ||
        (maxDepth.isDefined && pathLength >= maxDepth.get)) {
      goHome()
    } else {
      var randNextNodeId = takeRandomStep()
      if (onlyOnce && seenBefore(randNextNodeId)) {
        seenNodesTracker.clear()
        goHome()
      } else {
        randNextNodeId
      }
    }
    seenNodesTracker.recordInfo(nextNodeId, 0)
    pathLength += 1
    currNode = getExistingNodeById(graph, nextNodeId)

    currNode
  }

  def hasNext = true
}

/**
 * Same as RandomTraverser except that the number of steps taken is bounded by {@code maxSteps}
 */
class RandomBoundedTraverser(graph: Graph, dir: GraphDir, homeNodeIds: Seq[Int], val maxSteps: Long,
    walkParams: RandomWalkParams) extends RandomTraverser(graph, dir, homeNodeIds,
    walkParams.resetProbability, walkParams.maxNumEdgesThresh, walkParams.visitSameNodeOnce,
    walkParams.randNumGen, walkParams.maxDepth,
    walkParams.filterHomeNodeByNumEdges) with BoundedIterator[Node]

/**
 * A traverser that keeps track of distance from homeNodeIds. If {@code onlyOnce} is true,
 * the same node is not traversed again.
 */
abstract class DistanceTraverser[T](graph: Graph, homeNodeIds: Seq[Int], onlyOnce: Boolean)
    extends Traverser { self =>

  private val distanceTracker = new InfoKeeper[Int] {
    override val onlyOnce = self.onlyOnce
  }

  protected def init(defaultInfo: T) {
    homeNodeIds foreach { id => visitPotentialNode(id, defaultInfo, 0) }
  }

  protected def visitPotentialNode(id: Int, info: T, dist: Int) {
    val seen = seenBefore(id)
    if (!(onlyOnce && seen)) {
      storeForVisit(id, info)
    }
    if (!seen) distanceTracker.recordInfo(id, dist)
  }

  protected def storeForVisit(id: Int, info: T)

  protected def seenBefore(id: Int) = distance(id).isDefined

  def distance(id: Int) = distanceTracker.infoOfNode(id)
}

/**
 * Traverses in breadth-first order. This implies that first all the neighbors of
 * {@code homeNodeIds} are visited, then their neighbors, etc.
 * @param graph the graph to traverse on
 * @param dir direction in which to traverse
 * @param homeNodeIds the ids of nodes that the BFS starts
 * @param maxDepth the maximum depth of nodes to visit in BFS
 * @param numTopPathsPerNode number of top paths (in this case, previous neighbor nodes) to store
 * @param maxNumEdgesThresh threshold of the number of neighbors a node can have, if a node has more
 * than the threshold value, we skip its children in BFS
 * @param onlyOnce specifies whether the same node should only be allowed to be
 * visited once in any path
 */

// TODO replace qu

class BreadthFirstTraverser(graph: Graph, dir: GraphDir, homeNodeIds: Seq[Int],
                            maxDepth: Option[Int], maxNumEdgesThresh: Option[Int], maxSteps: Long,
                            onlyOnce: Boolean, prevNbrCounter: PrevNbrCounter)
    extends Traverser { self =>

  private val log = Logger.get
  // the number of items can be enqueued is bounded by maxSteps
  // cuz we never need to enqueue more than maxSteps items
  private var numEnqueuedEver = 0L
  private val qu = new IntArrayList

  private val depthTracker = new InfoKeeper[Int] {
    override val onlyOnce = self.onlyOnce

    override def recordInfo(id: Int, info: Int) {
      if (!infoPerNode.containsKey(id)) {
        infoPerNode.put(id, info)
      }
    }
  }

  homeNodeIds foreach { id =>
    depthTracker.recordInfo(id, 0)
    enqueueNeighbors(id, 1)
  }

  private def visitPotentialNode(id: Int, depth: Int) {
    if (!(onlyOnce && seen(id))) {
      qu.add(id)
      numEnqueuedEver += 1
    }
    depthTracker.recordInfo(id, depth)
  }

  private def enqueueNeighbors(nodeId: Int, newDepth: Int) {
    val nd = getExistingNodeById(graph, nodeId)
    if (maxNumEdgesThresh.isEmpty || (nd.neighborCount(dir) <= maxNumEdgesThresh.get)) {
      nd.neighborIds(dir) foreach { id =>
        // bound the total number of items that
        // can be pushed into the queue by maxSteps
        if (numEnqueuedEver < maxSteps) {
          visitPotentialNode(id, newDepth)
          prevNbrCounter.recordInfo(id, nodeId)
        }
      }
    }
  }

  private[graph] def depth(id: Int) = depthTracker.infoOfNode(id)
  private def seen(id: Int) = depth(id).isDefined

  def next = {
    val curr = qu.removeInt(0)
    val currDepth = depth(curr).get
    val nd = getExistingNodeById(graph, curr)
    log.ifTrace { "visiting %d, nbrCount=%d, maxNumEdges=%d, depth=%d".format(curr,
      nd.neighborCount(dir), maxNumEdgesThresh.getOrElse(-1), currDepth) }
    val newDepth = currDepth + 1
    enqueueNeighbors(curr, newDepth)
    nd
  }

  def hasNext = if (qu.isEmpty) {
    Stats.incr("bfs_walk_request_exhausted_2nd_degree", 1)
    false
  } else {
    maxDepth.isEmpty || depth(qu.getInt(0)).get <= maxDepth.get
  }
}

class DepthFirstTraverser(graph: Graph, dir: GraphDir, homeNodeIds: Seq[Int],
    onlyOnce: Boolean)
    extends DistanceTraverser[Int](graph, homeNodeIds, onlyOnce) {

  // stack keeps a tuple (node, child# of higher node in stack). Hence, top node
  // of stack always has child# = -1
  case class NodeDesc(val id: Int, val childOff: Int)
  private val stack = new mutable.Stack[NodeDesc]
  init(-1)

  protected def storeForVisit(id: Int, info: Int) { stack.push(NodeDesc(id, info)) }

  private def firstNotYetVisitedNode(nd: Node, off: Int): Option[NodeDesc] = {
    assert(off != -1)
    if (off == nd.neighborCount(dir)) {
      // no more children of this node left, go to its parent
      if (stack.isEmpty) None else {
        val topel = stack.pop
        firstNotYetVisitedNode(getExistingNodeById(graph, topel.id), topel.childOff + 1)
      }
    } else {
      val childId = nd.neighborIds(dir)(off)
      if (onlyOnce && seenBefore(childId)) {
        // try the next child
        firstNotYetVisitedNode(nd, off + 1)
      } else {
        storeForVisit(nd.id, off)
        Some(NodeDesc(childId, distance(nd.id).get + 1))
      }
    }
  }

  def next = {
    var NodeDesc(id, off) = stack.pop
    assert(off == -1)
    val topnd = getExistingNodeById(graph, id)
    val nextUnvisitedNode = firstNotYetVisitedNode(topnd, off + 1)
    nextUnvisitedNode match {
      case Some(NodeDesc(nodeId, dist)) => visitPotentialNode(nodeId, -1, dist)
      case None => // do nothing
    }
    topnd
  }

  def hasNext = !stack.isEmpty
}
