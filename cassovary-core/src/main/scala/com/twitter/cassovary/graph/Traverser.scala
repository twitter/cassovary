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
import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import com.twitter.cassovary.graph.tourist.{BoolInfoKeeper, IntInfoKeeper, PrevNbrCounter}
import com.twitter.logging.Logger
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue
import scala.annotation.tailrec
import scala.util.Random

/**
 * A Traverser traverses the graph in a certain order of nodes.
 */
trait Traverser[+V <: Node] extends Iterator[V] { self =>
  /**
   * Traverser only visits node Ids listed in some node's edges, and thus
   * we know that node must exist, safe to assume graph.getNodeById return non-None value
   */
  protected def getExistingNodeById[W >: V <: Node](graph: Graph[W], id: Int): W = graph.getNodeById(id).get
}

/**
 * Bounds an iterator to go no more than a specified maximum number of steps
 */
trait BoundedIterator[+T] extends Iterator[T] {
  /**
   * @return If option is defined, it is the maximal number of elements the iterator returns.
   *         If is `None`, the iterator is not bounded.
   */
  def maxSteps: Option[Long]

  private var numStepsTaken = 0L

  abstract override def next() = {
    numStepsTaken += 1
    super.next()
  }

  abstract override def hasNext = maxSteps match {
    case Some(max) if numStepsTaken >= max => false
    case _ => super.hasNext
  }
}

/**
 * Randomly traverse the graph, going from one node to a random neighbor in direction `dir`.
 * @param graph the graph to traverse on
 * @param dir direction in which to traverse
 * @param homeNodeIds the ids of nodes that the traverser will go to next if the node has no
 * neighbors, or we reset the traversal with probability `resetProbability`,
 * or if the number of out-going edges at the current node exceeds `maxNumEdgesThresh`
 * @param resetProbability the probability of teleporting back to home node set at each step
 * @param maxNumEdgesThresh if set, do not traverse edges with > maxNumEdgesThresh outgoing edges
 * @param onlyOnce specifies whether the same node should only be allowed to be visited once
 * in any path.
 * @param randNumGen a random number generator (for stable walk, a seeded random number
 * generator is used).
 * @param maxDepth if set, max depth of path
 * @param filterHomeNodeByNumEdges filter home node by number of edges
 */
class RandomTraverser[+V <: Node](graph: Graph[V], dir: GraphDir, homeNodeIds: Seq[Int],
                      resetProbability: Double, maxNumEdgesThresh: Option[Int], onlyOnce: Boolean,
                      randNumGen: Random, maxDepth: Option[Int], filterHomeNodeByNumEdges: Boolean)
  extends Traverser[V] {  self =>

  private[this] var currNode: V = _
  private val homeNodeIdSet = Set(homeNodeIds: _*)

  private val seenNodesTracker = new BoolInfoKeeper(self.onlyOnce)

  protected def seenBefore(id: Int) = seenNodesTracker.infoOfNode(id).isDefined
  private var pathLength = 0

  private def goHome(): Int = {
    pathLength = 0
    NodeUtils.pickRandNodeId(homeNodeIds, randNumGen)
  }

  private def takeRandomStep(): Int = {
    val nextRandom = randNumGen.nextDouble()
    val needToFilterByNumEdges = filterHomeNodeByNumEdges || !(homeNodeIdSet contains currNode.id)
    if (nextRandom < resetProbability ||
      (needToFilterByNumEdges && NodeUtils.hasTooManyEdges(dir, maxNumEdgesThresh)(currNode))) {
      goHome()
    } else {
      currNode.randomNeighbor(dir, randNumGen).getOrElse(goHome())
    }
  }

  def next() = {
    val nextNodeId = if (currNode == null ||
      (maxDepth.isDefined && pathLength >= maxDepth.get)) {
      goHome()
    } else {
      val randNextNodeId = takeRandomStep()
      if (onlyOnce && seenBefore(randNextNodeId)) {
        seenNodesTracker.clear()
        goHome()
      } else {
        randNextNodeId
      }
    }
    seenNodesTracker.recordInfo(nextNodeId, false)
    pathLength += 1
    currNode = getExistingNodeById(graph, nextNodeId)

    currNode
  }

  def hasNext = true
}

/**
 * Same as RandomTraverser except that the number of steps taken is bounded by `maxSteps`
 */
class RandomBoundedTraverser[+V <: Node](graph: Graph[V], dir: GraphDir, homeNodeIds: Seq[Int], maxStepss: Long,
    walkParams: RandomWalkParams) extends RandomTraverser(graph, dir, homeNodeIds,
    walkParams.resetProbability, walkParams.maxNumEdgesThresh, walkParams.visitSameNodeOnce,
    walkParams.randNumGen, walkParams.maxDepth,
    walkParams.filterHomeNodeByNumEdges) with BoundedIterator[V] {
  val maxSteps: Option[Long] = Some(maxStepss)
}

object Walk {
  /**
   * Optional limit used for limiting depth or degree of nodes that are added
   * to the queue.
   */
  class Limit(limit: Option[Int]) {
    def isLimitReached(value: Int): Boolean = limit match {
      case Some(d) if value >= d => true
      case _ => false
    }
  }

  /**
   * We use node colors in the QueueBasedTraverser. We use
   * 3 colors defined as `NodeColor`.
   */
  object NodeColor extends Enumeration {
    type Color = Value
    val Unenqueued = Value
    val Enqueued = Value
    val Visited = Value
  }

  /**
   * Stores information of three colors assigned to node ids (Ints).
   *
   * Initially every node has color `Unenqueued`. When adding it to the
   * queue for the first time, we mark it `Enqueued` and then `Visited`,
   * when visiting it for the first time.
   * There are no other color changes.
   *
   * Uses BoolInfoKeeper under the hood.
   */
  trait NodeColoring {

    import NodeColor._

    val map = new BoolInfoKeeper(false)

    def +=(kv: (Int, Color)) {
      kv._2 match {
        case Unenqueued => ()
        case Enqueued => if (get(kv._1) == Unenqueued) map.recordInfo(kv._1, false)
        case Visited => if (get(kv._1) != Visited) map.recordInfo(kv._1, true)
      }
    }

    def get(number: Int): Color = {
      map.infoOfNode(number) match {
        case None => Unenqueued
        case Some(false) => Enqueued
        case Some(true) => Visited
      }
    }
  }

  /**
   * Parameters used to limit Traversers defined below.
   *
   * @param maxDepth the maximum depth of nodes to visit
   * @param maxNumEdgesThreshold threshold of the number of neighbors a node can have, if a node has more
   *                             than the threshold value, we skip its children
   * @param maxSteps number of steps the traverser makes
   */
  case class Limits(maxDepth: Option[Int] = None, maxNumEdgesThreshold: Option[Int] = None,
                        maxSteps: Option[Long] = None) {
    def this(depth: Int, numEdges: Int, steps: Long) = this(Some(depth), Some(numEdges), Some(steps))
  }

}

/**
 * General schema for some Traversers (like BFS, DFS). 
 * `QueueBasedTraverser` keeps nodes to visit next in a queue. 
 * It iteratively visits nodes from the front of the queue in order based on the type of traversal
 * and optionally adds new nodes to the queue.
 */
trait QueueBasedTraverser[+V <: Node] extends Traverser[V] {
  import Walk._

  /**
   * Graph to be traversed.
   */
  def graph: Graph[V]

  /**
   * Nodes to start the traversal from.
   */
  def homeNodeIds: Seq[Int]

  /**
   * Direction of the walk.
   */
  def dir: GraphDir

  /**
   * The priority of nodes adding to the queue. `BFS` adds nodes at the
   * end and `DFS` adds nodes at the beginning.
   */
  object GraphTraverserNodePriority extends Enumeration {
    type GraphTraverserNodePriority = Value

    val FIFO = Value
    val LIFO = Value
  }

  import GraphTraverserNodePriority._

  /**
   * The priority of nodes adding to the queue.
   */
  def nodePriority: GraphTraverserNodePriority

  /**
   * Action performed when visiting node `node` (before iteratur retuns `node`).
   * Should be implemented in subclass.
   */
  def visitNode(node: Node): Unit = {}

  /**
   * Queue that stores nodes to be visited next.
   */
  protected val queue = new IntArrayFIFOQueue()

  /**
   * Number of nodes ever enqueued in the `queue`.
   */
  protected var numEnqueuedEver = 0L

  /**
   * Optional counter of previous nodes to a given node.
   */
  lazy val prevNbrCounter: Option[PrevNbrCounter] = None

  /**
   * Depth of visit of a given node.
   */
  def depth(node: Int): Option[Int]

  /**
   * We mark nodes using 3 'colors': `Unenqueued`, `Enqueued`, `Visited`.
   * Initially every node is implicitly marked with `Unenqueued`.
   *
   * Node is marked `Enqueued`, when we add it to the queue. We mark
   * it `Visited` just before returning with iterator's `next` function.
   */
  lazy val coloring = new Walk.NodeColoring {}

  /**
   * Checks if the node of a given color should be enqueued.
   */
  def shouldBeEnqueued(color: Walk.NodeColor.Color): Boolean

  /**
   * Set to true to deque a node from the queue before processing.
   */
  val shouldBeDequeuedBeforeProcessing: Boolean = true

  /**
   * Options to limit traverser walk.
   */
  lazy val limits: Walk.Limits = Walk.Limits()

  lazy val depthLimit: Limit = new Limit(limits.maxDepth)
  lazy val degreeLimit: Limit = new Limit(limits.maxNumEdgesThreshold)
  lazy val maxSteps: Option[Long] = limits.maxSteps

  /**
   * During initialization we enqueue `homeNodeIds`.
   */
  enqueue(homeNodeIds, None)

  /**
   * If the queue is longer than the number of nodes that can be visited due to
   * `maxSteps` bound, cuts the list of nodes that are to be added to the queue
   * in order not to add too many of them.
   *
   * Assumes that all nodes in the queue are being visited.
   */
  def limitAddedToQueue(nodes: Seq[Int]): Seq[Int] = {
    maxSteps match {
      case None => nodes
      case Some(max) => if (max - numEnqueuedEver > Int.MaxValue) {
        nodes
      } else {
        nodes.take((max - numEnqueuedEver).toInt)
      }
    }
  }

  /**
   * Returns nodes ids that should be added to the queue after visiting `node`.
   */
  def chooseNodesToEnqueue(node: Node): Seq[Int] = {
    val currDepth = depth(node.id).get

    if (depthLimit.isLimitReached(currDepth) ||
      degreeLimit.isLimitReached(node.neighborCount(dir))) {
      Seq()
    } else {
      limitAddedToQueue(node.neighborIds(dir).filter(n => shouldBeEnqueued(coloring.get(n))))
    }
  }

  /**
   * Enqueues `nodes` added after `from` node is visited (or None
   * when equeueing initial nodes).
   */
  protected def enqueue(nodes: Seq[Int], from: Option[Int]): Unit = {
    numEnqueuedEver += nodes.size
    nodes.foreach {
      node => coloring += (node, NodeColor.Enqueued)
    }
    if (prevNbrCounter.isDefined && from.isDefined) {
      nodes.foreach {
        node => prevNbrCounter.get.recordPreviousNeighbor(node, from.get)
      }
    }
    nodePriority match {
      case GraphTraverserNodePriority.LIFO =>
        nodes.reverse.foreach (node => queue.enqueueFirst(node))
      case GraphTraverserNodePriority.FIFO =>
        nodes.foreach (node => queue.enqueue(node))
    }
  }

  /**
   * Performs action needed when visiting a node.
   */
  protected def processNode(node : Node): Unit = {
    visitNode(node)
    coloring += (node.id, NodeColor.Visited)
    enqueue(chooseNodesToEnqueue(node), Some(node.id))
  }

  /**
   * Finds in the queue the node that will be visited next.
   */
  protected def findNextNodeToVisit(): Option[Int] = {
    if (queue.isEmpty)
      None
    else
      Some(queue.firstInt())
  }

  def next() = {
    findNextNodeToVisit() match {
      case Some(nextId) =>
        if (shouldBeDequeuedBeforeProcessing) queue.dequeueInt()
        val node = getExistingNodeById(graph, nextId)
        processNode(node)
        node
      case None =>
        Iterator.empty.next()
    }
  }

  def hasNext = findNextNodeToVisit().isDefined
}

/**
 * A traverser that keeps track of first depth of visiting a given node.
 */
trait DepthTracker extends QueueBasedTraverser[Node] {

  private lazy val depthTracker = new IntInfoKeeper(true)

  abstract override protected def enqueue(nodes: Seq[Int], from: Option[Int]): Unit = {
    val fromDepth = from.flatMap(depthTracker.infoOfNode).getOrElse(-1)
    nodes foreach { id =>
      depthTracker.recordInfo(id, fromDepth + 1)
    }
    super.enqueue(nodes, from)
  }

  def allDepths: collection.Map[Int, Int] = depthTracker.infoAllNodes

  def depth(id: Int) = depthTracker.infoOfNode(id)
}

/**
 * Traverses in BFS order. This implies that first all the neighbors of
 * `homeNodeIds` are visited, then their neighbors, etc.
 * @param graph the graph to traverse on
 * @param dir direction in which to traverse
 * @param homeNodeIds the ids of nodes that the walk starts from
 * @param walkLimits limiting parameters
 * @param bfsPrevNbrCounter if set, tracks previous neighbors by occurrence
 */
class BreadthFirstTraverser[+V <: Node](val graph: Graph[V], val dir: GraphDir, val homeNodeIds: Seq[Int],
                            walkLimits: Walk.Limits = Walk.Limits(),
                            bfsPrevNbrCounter: Option[PrevNbrCounter] = None)
  extends DepthTracker
  with BoundedIterator[V]
{
  private val log = Logger.get()

  override def nodePriority = GraphTraverserNodePriority.FIFO

  override lazy val limits = walkLimits

  override lazy val prevNbrCounter = bfsPrevNbrCounter

  override def visitNode(node: Node): Unit = {
    val currDepth = depth(node.id).get
    log.ifTrace { "visiting %d, nbrCount=%d, maxNumEdges=%d, depth=%d".format(node.id,
      node.neighborCount(dir), limits.maxNumEdgesThreshold.getOrElse(-1), currDepth) }
    super.visitNode(node)
  }

  def shouldBeEnqueued(color: Walk.NodeColor.Color): Boolean = {
    color == Walk.NodeColor.Unenqueued
  }
}

/**
 * Traverses in FIFO (breadth first) order but without any limit on the
 * number of times every node can be visited.
 * Every time the walk visits a node, it adds all its neighbors (along direction `dir`)
 * to the queue.
 * @param graph the graph to traverse on
 * @param dir direction in which to traverse
 * @param homeNodeIds the ids of nodes that the walk starts from
 * @param walkLimits limiting parameters
 * @param apwPrevNbrCounter if set, tracks previous neighbors by occurrence
 */
class AllPathsWalk[+V <: Node](val graph: Graph[V], val dir: GraphDir, val homeNodeIds: Seq[Int],
                   walkLimits: Walk.Limits = Walk.Limits(),
                   apwPrevNbrCounter: Option[PrevNbrCounter] = None)
  extends DepthTracker
  with BoundedIterator[V]
{
  val onlyOnce = false

  override lazy val limits = walkLimits

  override lazy val prevNbrCounter = apwPrevNbrCounter

  override def nodePriority = GraphTraverserNodePriority.FIFO

  override def shouldBeEnqueued(color: Walk.NodeColor.Color): Boolean = true
}

/**
 * Traverses in DFS order (every node can be visited only once).
 * @param graph the graph to traverse on
 * @param dir direction in which to traverse
 * @param homeNodeIds the ids of nodes that the walk starts from
 * @param walkLimits limiting parameters
 */
class DepthFirstTraverser[+V <: Node](val graph: Graph[V], val dir: GraphDir, val homeNodeIds: Seq[Int],
                                walkLimits: Walk.Limits = Walk.Limits())
    extends DepthTracker
    with BoundedIterator[V]
{
  private val log = Logger.get()

  override lazy val limits = walkLimits

  override def nodePriority = GraphTraverserNodePriority.LIFO

  override def visitNode(node: Node): Unit = {
    log.ifTrace { "visiting %d, nbrCount=%d, depth=%d".format(node.id,
      node.neighborCount(dir), depth(node.id).get) }
    super.visitNode(node)
  }

  /**
   * Seeks the next node that should be visited. We may need to skip
   * some nodes, because it is possible that we add a node to the queue twice.
   *
   * Takes already visited nodes from the queue.
   * Does not take the next node from the queue (in order to allow hasNext checking).
   * */
   @tailrec
   override final def findNextNodeToVisit(): Option[Int] = {
    if (queue.isEmpty) {
      None
    } else {
      val next = queue.firstInt()
      val visitedBefore = coloring.get(next) match {
        case Walk.NodeColor.Visited => true
        case _ => false
      }
      if (visitedBefore) {
        handleVisitedInQueue(next)
        findNextNodeToVisit()
      } else {
        Some(next)
      }
    }
  }

  def handleVisitedInQueue(next: Int) {
    queue.dequeueInt()
  }

  // can't limit number of nodes added to the queue, because we skip some nodes
  override def limitAddedToQueue(nodes: Seq[Int]): Seq[Int] = nodes

  override def shouldBeEnqueued(color: Walk.NodeColor.Color): Boolean = color != Walk.NodeColor.Visited
}


/**
 * A trait to be mixed in to the DepthFirstTraverser that keeps track of visiting distance
 * from `homeNodeIds`.
 *
 * Note that DepthTracker for the DFS case returns the depth of first seeing a particular
 * node. It does not have to be the same as the depth at which the node is visited.
 */
trait PathLengthTracker extends DepthFirstTraverser[Node] {
  private lazy val nextVisitDistanceTracker = new IntInfoKeeper(false)
  private lazy val visitDistanceTracker = new IntInfoKeeper(true)

  abstract override protected def enqueue(nodes: Seq[Int], from: Option[Int]): Unit = {
    val fromDistance = from.flatMap(visitDistanceTracker.infoOfNode).getOrElse(-1)
    nodes foreach { id =>
      nextVisitDistanceTracker.recordInfo(id, fromDistance + 1)
    }
    super.enqueue(nodes, from)
  }

  override def visitNode(node: Node) {
    visitDistanceTracker.recordInfo(node.id, nextVisitDistanceTracker.infoOfNode(node).get)
    super.visitNode(node)
  }

  def distance(id: Int) = visitDistanceTracker.infoOfNode(id)
}

/**
 * Trait to be mixed in to the BFS/DFS traverser to add discovery and finishing
 * times of a node.
 *
 * Discovery time of a node is time when the node is added to the queue for the first time.
 *
 * Finishing time of a node is time just after all `dir` neighbors of the node became finished
 * (or when it is visited if it has no `dir` neighbors that should be processed later).
 */
trait DiscoveryAndFinishTimeTracker extends DepthFirstTraverser[Node] {
  private[this] var time: Int = _ // automatically initialized to 0 before first enqueue

  private lazy val discoveryTime = new IntInfoKeeper(false)

  private lazy val finishingTime = new IntInfoKeeper(false)

  override val shouldBeDequeuedBeforeProcessing = false

  abstract override protected def enqueue(nodes: Seq[Int], from: Option[Int]): Unit = {
    nodes.foreach {
      node =>
        if (discoveryTime(node).isEmpty) {
          discoveryTime.recordInfo(node, time)
          time += 1
        }
    }
    super.enqueue(nodes, from)
  }

  override def handleVisitedInQueue(node: Int): Unit = {
    if (finishingTime(node).isEmpty) {
      finishingTime.recordInfo(node, time)
      time += 1
    }
    super.handleVisitedInQueue(node)
  }

  /**
   * @return Number of nodes discovered or finished before discovering node with `id`.
   */
  def discoveryTime(id: Int): Option[Int] = {
    discoveryTime.infoOfNode(id)
  }

  /**
   * @return Number of nodes discovered or finished before finishing node with `id`.
   */
  def finishingTime(id: Int): Option[Int] = {
    finishingTime.infoOfNode(id)
  }
}
