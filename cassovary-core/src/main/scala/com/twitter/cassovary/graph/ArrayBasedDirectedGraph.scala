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

import java.util.concurrent.atomic.AtomicInteger

import com.google.common.annotations.VisibleForTesting
import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.graph.node._
import com.twitter.cassovary.util.BoundedFuturePool
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.Future.when
import com.twitter.util.{Await, Future, FuturePool}

import scala.collection.mutable

/**
 * Construct an array based directed graph based on a set of edges. The graph can be constructed in
 * the following ways based on the stored direction:
 * 1. If OnlyIn or OnlyOut edges need to be kept, supply only those edges in the edges iterableSeq
 * 2. If BothInOut edges need to be kept, supply only the outgoing edges in the edges iterableSeq
 */

/**
 * This case class holds a node's id, all its out edges, and the max
 * id of itself and ids of nodes in its out edges
 */
case class NodeIdEdgesMaxId(id: Int, edges: Array[Int], maxId: Int)

object NodeIdEdgesMaxId {
  def apply(id: Int, edges: Array[Int]) =
    new NodeIdEdgesMaxId(id, edges, edges.foldLeft[Int](id)((x, y) => x max y))
}

/**
 * ArrayBasedDirectedGraph can be stored with neighbors sorted or not. Therefore there
 * are 3 strategies of loading a graph from input:
 *   `AlreadySorted` - creates a graph with sorted neighbors from sorted input
 *   `SortWhileReading` - creates a graph with sorted neighbors sorting them
 *                        while reading
 *   `LeaveUnsorted` - creates graph with unsorted neighbors (default)
 */
object NeighborsSortingStrategy extends Enumeration {
  type NeighborsSortingStrategy = Value

  val AlreadySorted = Value
  val SortWhileReading = Value
  val LeaveUnsorted = Value
}

object ArrayBasedDirectedGraph {
  import NeighborsSortingStrategy._

  def apply(iteratorSeq: Seq[Iterable[NodeIdEdgesMaxId]],
            parallelismLimit: Int,
            storedGraphDir: StoredGraphDir,
            neighborsSortingStrategy: NeighborsSortingStrategy = LeaveUnsorted):
  ArrayBasedDirectedGraph = {
    val constructor = new ArrayBasedDirectedGraphConstructor(iteratorSeq,
                                                             parallelismLimit,
                                                             storedGraphDir,
                                                             neighborsSortingStrategy)
    constructor()
  }

  @VisibleForTesting
  def apply(iterable: Iterable[NodeIdEdgesMaxId],
            storedGraphDir: StoredGraphDir,
            neighborsSortingStrategy: NeighborsSortingStrategy): ArrayBasedDirectedGraph = {
    apply(Seq(iterable), 1, storedGraphDir, neighborsSortingStrategy)
  }

  // a private convenience class encapsulating a representation of the collection of nodes
  private class NodeCollection(val maxNodeId: Int, numNodesEstimate: Int, val numEdges: Long) {
    private val table = new Array[Node](maxNodeId + 1)
    private var _numNodes = 0
    private val nodeIds = new mutable.BitSet(maxNodeId + 1)
    private var inEdgeSizes: Array[AtomicInteger] = _

    def mark(id: Int): Unit = {
      nodeIds += id
    }

    def nodeIdsIterator = nodeIds.iterator
    def nodesIterator = nodeIdsIterator map { id => table(id) }

    def get(id: Int) = table(id)
    def add(node: Node): Unit = {
      val id = node.id
      assert(table(id) == null, s"Duplicate node $id detected")
      table(id) = node
      mark(id)
      _numNodes += 1
    }

    def addInEdge(dest: Int, source: Int): Unit = {
      val inEdgeIndex = inEdgeSizes(dest).getAndIncrement
      table(dest).asInstanceOf[FillingInEdgesBiDirectionalNode].inEdges(inEdgeIndex) = source
    }

    def createAtomicInts(): Unit = {
      inEdgeSizes = Array.tabulate[AtomicInteger](maxNodeId + 1) {
        i => if (nodeIds.contains(i)) new AtomicInteger() else null
      }
    }

    def incEdgeSize(id: Int) { inEdgeSizes(id).incrementAndGet() }
    def getAndResetEdgeSize(id: Int) = {
      val sz = inEdgeSizes(id).intValue()
      if (sz > 0) inEdgeSizes(id).set(0)
      sz
    }

    def numNodes = _numNodes

  }

  /**
   * Constructs array based directed graph
   */
  private class ArrayBasedDirectedGraphConstructor(
    iterableSeq: Seq[Iterable[NodeIdEdgesMaxId]],
    parallelismLimit: Int,
    storedGraphDir: StoredGraphDir,
    neighborsSortingStrategy: NeighborsSortingStrategy
  ) {
    private lazy val log = Logger.get()
    private val statsReceiver = DefaultStatsReceiver

    private val emptyArray = Array[Int]()

    private val futurePool = new BoundedFuturePool(FuturePool.unboundedPool, parallelismLimit)

    /**
     * This case class holds either a part of the total graph loaded in one thread
     * (T = Node) or the whole graph (T = Seq[Node])
     */
    private case class GraphInfo[T](nodesOutEdges: Seq[T], //nodes explicitly given to us
        maxNodeId: Int, // across all nodes (explicitly or implicitly given)
        numNodes: Int, // only number of nodes explicitly given to us
        numEdges: Long)

    /**
     * Construct an array-based graph from an sequence of `NodeIdEdgesMaxId` iterables
     * This function builds the array-based graph from a seq of nodes with out edges
     * using the following steps:
     * 0. read from file and construct a sequence of Nodes
     * 1. create an array of nodes and mark all positions in the array where there is a node
     * 2. instantiate nodes that only have in-edges (thus has not been created in the input)
     * next steps apply only if in-edge is needed
     * 3. calculate in-edge array sizes
     * 4. (if in-edge dir only) remove out edges
     * 5. instantiate in-edge arrays
     * 6. iterate over the sequence of nodes again, instantiate in-edges
     */
    def apply(): ArrayBasedDirectedGraph = {

      val result: Future[ArrayBasedDirectedGraph] = for {
        graphInfo <- fillOutEdges(iterableSeq)
        nodeCollection <- markStoredNodes(graphInfo)
        nodesWithNoOutEdges <- createNodesWithNoOutEdges(nodeCollection)
        _ <- when(storedGraphDir == StoredGraphDir.BothInOut) {
          fillMissingInEdges(nodeCollection, graphInfo.nodesOutEdges, nodesWithNoOutEdges)
        }
      } yield
        new ArrayBasedDirectedGraph(nodeCollection, storedGraphDir)

      Await.result(result)
    }

    /**
     * Reads `iterableSeq`'s edges, creates nodes and puts them in an `ArrayBuffer[Seq[Node]]`.
     * In every node only edges directly read from input are set.
     * @return Future with information for the graph
     */
    private def fillOutEdges(iterableSeq: Seq[Iterable[NodeIdEdgesMaxId]]):
    Future[GraphInfo[Seq[Node]]] = {
      log.debug("loading nodes and out edges from file in parallel")
      val nodesOutEdges = new mutable.ArrayBuffer[Seq[Node]]
      var maxNodeIdAll = 0
      var numEdgesAll = 0L
      var numNodesAll = 0

      val outEdgesAll: Future[Seq[GraphInfo[Node]]] = statsReceiver.time(
        "graph_dump_load_partial_nodes_and_out_edges_parallel") {
        Future.collect(iterableSeq.map(i => readOutEdges(i.iterator)))
      }

      outEdgesAll.map {
        // aggregate across parts
        case outEdgesOnePart => outEdgesOnePart.foreach {
          case GraphInfo(nodesInPart, maxIdInPart, numNodesInPart, numEdgesInPart) =>
            nodesOutEdges += nodesInPart
            maxNodeIdAll = maxNodeIdAll max maxIdInPart
            numNodesAll += numNodesInPart
            numEdgesAll += numEdgesInPart
        }
        GraphInfo[Seq[Node]](nodesOutEdges, maxNodeId = maxNodeIdAll,
          numNodes = numNodesAll, numEdges = numEdgesAll)
      }
    }

    /**
     * Reads out edges from iterator and returns `GraphInfo` object.
     */
    private def readOutEdges(iterator: Iterator[NodeIdEdgesMaxId]):
    Future[GraphInfo[Node]] = futurePool {
      statsReceiver.time("graph_load_read_out_edge_from_dump_files") {
        val nodesWithEdges = new mutable.ArrayBuffer[Node]
        var newMaxId = 0
        var numEdges = 0L

        iterator foreach { item =>
          val id = item.id
          newMaxId = newMaxId max item.maxId
          numEdges += item.edges.length
          val edges = if (neighborsSortingStrategy == SortWhileReading) item.edges.sorted else item.edges
          val newNode = ArrayBasedDirectedNode(id, edges, storedGraphDir,
            neighborsSortingStrategy != LeaveUnsorted)
          nodesWithEdges += newNode
        }
        GraphInfo[Node](nodesWithEdges, maxNodeId = newMaxId, numNodes = nodesWithEdges.length,
          numEdges = numEdges)
      }
    }

    /**
     * Marks all nodes existing in the graph and creates nodes that have `storedGraphDir` consistent
     * edges.
     * @return a representation of the collection of nodes
     */
    private def markStoredNodes(graphInfo: GraphInfo[Seq[Node]]): Future[NodeCollection] = {
      val nodeCollection = new NodeCollection(graphInfo.maxNodeId, graphInfo.numNodes,
        graphInfo.numEdges)
      log.debug("in markStoredNodes")
      statsReceiver.time("graph_load_mark_ids_of_stored_nodes") {
        Future.join(
          graphInfo.nodesOutEdges.map(nodes =>
            futurePool {
              nodes foreach { node =>
                nodeCollection.add(node)
                storedGraphDir match {
                  case StoredGraphDir.OnlyIn =>
                    node.inboundNodes foreach nodeCollection.mark
                  case _ =>
                    node.outboundNodes foreach nodeCollection.mark
                }
              }
            })
        ).map(_ => nodeCollection)
      }
    }

    /**
     * Creates and returns nodes that have no out edges.
     * TODO (possible optimization): don't iterate over all marked nodes if we remove those
     * ids from nodeColl's bitset those ids that we have created nodes already for.
     */
    private def createNodesWithNoOutEdges(nodeColl: NodeCollection): Future[Seq[Node]] = futurePool {
      val nodesWithNoOutEdges = new mutable.ArrayBuffer[Node]()
      log.debug("creating nodes that have only in-coming edges")
      statsReceiver.time("graph_load_creating_nodes_without_out_edges") {
        nodeColl.nodeIdsIterator foreach { id =>
            val existingNode = nodeColl.get(id)
            if (existingNode == null) {
              val node = ArrayBasedDirectedNode(id, emptyArray, storedGraphDir,
                neighborsSortingStrategy != LeaveUnsorted)
              nodeColl.add(node)
              if (storedGraphDir == StoredGraphDir.BothInOut)
                nodesWithNoOutEdges += node
            }
          }
      }
      nodesWithNoOutEdges
    }

    private def fillMissingInEdges(nodeColl: NodeCollection, nodesOutEdges: Seq[Seq[Node]],
                                   nodesWithNoOutEdges: Seq[Node]):
    Future[Unit] = {
      log.debug("calculating in edges sizes")

       // Calculates sizes of incoming edges arrays.
      def findInEdgesSizes(nodesOutEdges: Seq[Seq[Node]]): Future[Unit] = {
        statsReceiver.time("graph_load_find_in_edge_sizes") {
          nodeColl.createAtomicInts()

          val futures = nodesOutEdges map {
            nodes => futurePool {
              nodes foreach {
                node => node.outboundNodes foreach { outEdge =>
                  nodeColl.incEdgeSize(outEdge)
                }
              }
            }
          }

          Future.join(futures)
        }
      }

      def instantiateInEdges(): Future[Unit] = {
        log.debug("instantiate in edges")
        statsReceiver.time("graph_load_instantiate_in_edge_arrays") {
          val futures = (nodesOutEdges.iterator ++ Iterator(nodesWithNoOutEdges)).map {
            (nodes: Seq[Node]) => futurePool {
              nodes foreach { node =>
               // reset inEdgesSizes, and use it as index pointer of
               // the current insertion place when adding in edges
                val edgeSize = nodeColl.getAndResetEdgeSize(node.id)
                if (edgeSize > 0) {
                  node.asInstanceOf[FillingInEdgesBiDirectionalNode].createInEdges(edgeSize)
                }
              }
            }
          }.toSeq
          Future.join(futures)
        }
      }

      def populateInEdges(): Future[Unit] = {
        log.debug("populate in edges")
        statsReceiver.time("graph_load_read_in_edge_from_dump_files") {
          val futures = nodesOutEdges.map {
            (nodes: Seq[Node]) => futurePool {
              nodes foreach { node =>
                node.outboundNodes foreach { outEdge =>
                  nodeColl.addInEdge(outEdge, node.id)
                }
              }
            }
          }
          Future.join(futures)
        }
      }

      def finishInEdgesFilling(): Future[Unit] = {
        log.debug("finishing filling")
        statsReceiver.time("finishing_filling_in_edges") {
          val futures = nodesOutEdges.map {
            nodes => futurePool {
              nodes.foreach {
                node =>
                  node.asInstanceOf[FillingInEdgesBiDirectionalNode].sortInNeighbors()
              }
            }
          }
          Future.join(futures)
        }
      }

      for {
        _ <- findInEdgesSizes(nodesOutEdges)
        _ <- instantiateInEdges()
        _ <- populateInEdges()
        _ <- when(neighborsSortingStrategy != LeaveUnsorted) (finishInEdgesFilling())
      } yield ()

    }
  }
}


/**
 * This class is an implementation of the directed graph trait that is backed by an array
 * The private constructor takes as its input a list of (@see Node) nodes, then stores
 * nodes in an array. It also builds all edges which are also stored in array.
 *
 * @param nodes the collection of nodes with edges instantiated
 * @param storedGraphDir the graph direction(s) stored
 */
class ArrayBasedDirectedGraph private (nodeCollection: ArrayBasedDirectedGraph.NodeCollection,
                              val storedGraphDir: StoredGraphDir) extends DirectedGraph[Node] {

  override lazy val maxNodeId = nodeCollection.maxNodeId

  val nodeCount = nodeCollection.numNodes
  val edgeCount = nodeCollection.numEdges

  def iterator = nodeCollection.nodesIterator

  def getNodeById(id: Int) = {
    if ( (id < 0) || (id > maxNodeId)) {
      None
    } else {
      val node = nodeCollection.get(id)
      if (node == null) {
        None
      } else {
        Some(node)
      }
    }
  }
}
