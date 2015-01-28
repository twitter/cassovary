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

import com.google.common.annotations.VisibleForTesting
import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.graph.node._
import com.twitter.cassovary.util.BoundedFuturePool
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.{Await, FuturePool, Future}
import java.util.concurrent.atomic.AtomicInteger
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
case class NodeIdEdgesMaxId(var id: Int, var edges: Array[Int], var maxId: Int)

object NodeIdEdgesMaxId {
  def apply(id: Int, edges: Array[Int]) =
    new NodeIdEdgesMaxId(id, edges, edges.foldLeft[Int](id)((x, y) => x max y))
}

/**
 * This case class holds a part of the total graph loaded in one thread
 * it consists of a seq of nodes with out-edges, a max overall id, and
 * a max id of nodes with out-edges
 */
private case class NodesMaxIds(nodesInOnePart: Seq[Node],
                               maxIdInPart: Int, nodeWithOutEdgesMaxIdInPart: Int)

object ArrayBasedDirectedGraph {
  def apply(iterableSeq: Seq[Iterable[NodeIdEdgesMaxId]], parallelismLimit: Int,
            storedGraphDir: StoredGraphDir):
  ArrayBasedDirectedGraph = {
    val constructor = new ArrayBasedDirectedGraphConstructor(iterableSeq, parallelismLimit, storedGraphDir)
    constructor()
  }

  @VisibleForTesting
  def apply(iterable: Iterable[NodeIdEdgesMaxId],
            storedGraphDir: StoredGraphDir): ArrayBasedDirectedGraph = apply(Seq(iterable), 1, storedGraphDir)

  /**
   * Constructs array based directed graph
   */
  private class ArrayBasedDirectedGraphConstructor(
    iterableSeq: Seq[Iterable[NodeIdEdgesMaxId]],
    parallelismLimit: Int,
    storedGraphDir: StoredGraphDir
  ) {
    private lazy val log = Logger.get()
    private val statsReceiver = DefaultStatsReceiver

    private val futurePool = new BoundedFuturePool(FuturePool.unboundedPool, parallelismLimit)

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
        (nodesOutEdges, maxNodeId, nodeWithOutEdgesMaxId) <- fillOutEdges(iterableSeq, storedGraphDir)
        (table, nodeIdSet) <- markStoredNodes(nodesOutEdges, maxNodeId, storedGraphDir)
        NodesWithNoOutEdgesAndGraphStats(nodesWithNoOutEdges, nodeWithOutEdgesCount, numEdges, numNodes) =
        createNodesWithNoOutEdges(table, nodeIdSet, maxNodeId, storedGraphDir)
        _ <- Future.when(storedGraphDir == StoredGraphDir.BothInOut) {
          fillMissingInEdges(table, nodesOutEdges, nodesWithNoOutEdges, nodeIdSet, maxNodeId)
        }
      } yield
        new ArrayBasedDirectedGraph(table.asInstanceOf[Array[Node]], maxNodeId,
          nodeWithOutEdgesMaxId, nodeWithOutEdgesCount, numNodes, numEdges, storedGraphDir)

      Await.result(result)
    }

    /**
     * Reads `iterableSeq`'s edges, creates nodes and puts them in an `ArrayBuffer[Seq[Node]]`.
     * In every node only edges directly read from input are set.
     * @return Future with read edges of type `Buffer[Seq[Node]]`, max node id and nodeWithOutEdgesMaxId
     */
    private def fillOutEdges(iterableSeq: Seq[Iterable[NodeIdEdgesMaxId]], storedGraphDir: StoredGraphDir):
    Future[(mutable.Buffer[Seq[Node]], Int, Int)] = {
      log.debug("loading nodes and out edges from file in parallel")
      val nodesOutEdges = new mutable.ArrayBuffer[Seq[Node]]
      var maxNodeId = 0
      var nodeWithOutEdgesMaxId = 0

      val outEdges: Future[Seq[NodesMaxIds]] = statsReceiver.time(
        "graph_dump_load_partial_nodes_and_out_edges_parallel") {
        Future.collect(iterableSeq.map(i => readOutEdges(i.iterator, storedGraphDir)))
      }

      outEdges.map {
        case outEdgesList => outEdgesList.foreach {
          case NodesMaxIds(nodesInOnePart, maxIdInPart, nodeWithOutEdgesMaxIdInPart) =>
            nodesOutEdges += nodesInOnePart
            maxNodeId = maxNodeId max maxIdInPart
            nodeWithOutEdgesMaxId = nodeWithOutEdgesMaxId max nodeWithOutEdgesMaxIdInPart
        }
          (nodesOutEdges, maxNodeId, nodeWithOutEdgesMaxId)
      }
    }

    /**
     * Reads out edges from iterator and returns `NodesMaxIds` object.
     */
    private def readOutEdges(iterator: Iterator[NodeIdEdgesMaxId], storedGraphDir: StoredGraphDir):
    Future[NodesMaxIds] = futurePool {
      statsReceiver.time("graph_load_read_out_edge_from_dump_files") {
        val nodes = new mutable.ArrayBuffer[Node]
        var newMaxId = 0
        var varNodeWithOutEdgesMaxId = 0
        var id = 0
        var edgesLength = 0

        iterator foreach { item =>
          id = item.id
          newMaxId = newMaxId max item.maxId
          varNodeWithOutEdgesMaxId = varNodeWithOutEdgesMaxId max item.id
          val edges = item.edges
          edgesLength = edges.length
          val newNode = ArrayBasedDirectedNode(id, edges, storedGraphDir)
          nodes += newNode
        }
        NodesMaxIds(nodes, newMaxId, varNodeWithOutEdgesMaxId)
      }
    }


    /**
     * Marks all nodes existing in the graph and creates nodes that have `storedGraphDir` consistent
     * edges.
     * @return array of created nodes and array of bytes - an indicator of nodes existence in a graph
     */
    private def markStoredNodes(nodesOutEdges: Seq[Seq[Node]], maxNodeId: Int, storedGraphDir: StoredGraphDir):
    Future[(Array[Node], Array[Byte])] = {
      val table = new Array[Node](maxNodeId + 1)
      val nodeIdSet = new Array[Byte](maxNodeId + 1)
      log.debug("mark the ids of all stored nodes in nodeIdSet")
      statsReceiver.time("graph_load_mark_ids_of_stored_nodes") {
        Future.join(
          nodesOutEdges.map(n =>
            futurePool {
              markExistingAndCreateWithOutEdgesNodes(n, table, nodeIdSet, storedGraphDir)
            })
        ).map(_ => (table, nodeIdSet))
      }
    }

    /**
     * Marks in `nodeIdSet` nodes from `nodes` that are in the graph and copies new nodes that
     * have outgoing edges to `table`.
     * @return Future of unit that completes, when all nodes are marked and created.
     */
    private def markExistingAndCreateWithOutEdgesNodes(nodes: Seq[Node], table: Array[Node],
                                                       nodeIdSet: Array[Byte], storedGraphDir: StoredGraphDir) {
      nodes foreach { node =>
        val nodeId = node.id
        assert(table(nodeId) == null, "Duplicate node detected. (" + nodeId + ")")
        table(nodeId) = node
        nodeIdSet(nodeId) = 1
        storedGraphDir match {
          case StoredGraphDir.OnlyIn =>
            node.inboundNodes foreach { inEdge => nodeIdSet(inEdge) = 1}
          case _ =>
            node.outboundNodes foreach { outEdge => nodeIdSet(outEdge) = 1}
        }
      }
    }

    case class NodesWithNoOutEdgesAndGraphStats(
      nodesWithNoOutEdges: mutable.ArrayBuffer[Node],
      nodeWithOutEdgesCount: Int,
      numEdges: Long,
      numNodes: Int
    )

    /**
     * Creates nodes that have no out edges and counts following graph properties: number of nodes,
     * number of edges, number of nodes with out edges.
     *
     * @return `NodeWithNoOutEdgesAndGraphStats` object that wraps all the data
     */
    private def createNodesWithNoOutEdges(table: Array[Node], nodeIdSet: Array[Byte], maxNodeId: Int,
                                          storedGraphDir: StoredGraphDir): NodesWithNoOutEdgesAndGraphStats = {
      val nodesWithNoOutEdges = new mutable.ArrayBuffer[Node]()
      var nodeWithOutEdgesCount = 0
      var numEdges = 0L
      var numNodes = 0
      log.debug("creating nodes that have only in-coming edges")
      statsReceiver.time("graph_load_creating_nodes_without_out_edges") {
        for (id <- 0 to maxNodeId) {
          if (nodeIdSet(id) == 1) {
            numNodes += 1
            if (table(id) == null) {
              val node = ArrayBasedDirectedNode(id, ArrayBasedDirectedNode.noNodes, storedGraphDir)
              table(id) = node
              if (storedGraphDir == StoredGraphDir.BothInOut)
                nodesWithNoOutEdges += node
            } else {
              nodeWithOutEdgesCount += 1
              storedGraphDir match {
                case StoredGraphDir.OnlyIn =>
                  numEdges += table(id).inboundNodes().size
                case _ =>
                  numEdges += table(id).outboundNodes().size
              }
            }
          }
        }
      }
      NodesWithNoOutEdgesAndGraphStats(nodesWithNoOutEdges, nodeWithOutEdgesCount, numEdges, numNodes)
    }

    /**
     * Fills missing InEdges when StoredGraphDir is BothInOut in `table` Array.
     *
     * @return Future of unit that is completed, when all missing in edges are filled in nodes from the `table`.
     */
    private def fillMissingInEdges(table: Array[Node], nodesOutEdges: Seq[Seq[Node]],
                                   nodesWithNoOutEdges: Seq[Node], nodeIdSet: Array[Byte], numNodes: Int):
    Future[Unit] = {
      log.debug("calculating in edges sizes")

      def instantiateInEdges(inEdgesSizes: Array[AtomicInteger]): Future[Unit] = {
        log.debug("instantiate in edges")
        statsReceiver.time("graph_load_instantiate_in_edge_arrays") {
          val futures = (nodesOutEdges.iterator ++ Iterator(nodesWithNoOutEdges)).map {
            (nodes: Seq[Node]) => futurePool {
              nodes foreach { node =>
                val biDirNode = node.asInstanceOf[BiDirectionalNode]
                val nodeId = biDirNode.id
                val edgeSize = inEdgesSizes(nodeId).intValue()
                if (edgeSize > 0)
                  biDirNode.inEdges = new Array[Int](edgeSize)
                // reset inEdgesSizes, and use it as index pointer of
                // the current insertion place when adding in edges
                inEdgesSizes(nodeId).set(0)
              }
            }
          }.toSeq
          Future.join(futures)
        }
      }

      def populateInEdges(inEdgesSizes: Array[AtomicInteger]): Future[Unit] = {
        log.debug("populate in edges")
        statsReceiver.time("graph_load_read_in_edge_from_dump_files") {
          val futures = nodesOutEdges.map {
            (nodes: Seq[Node]) => futurePool {
              nodes foreach { node =>
                node.outboundNodes foreach { outEdge =>
                  val index = inEdgesSizes(outEdge).getAndIncrement
                  table(outEdge).asInstanceOf[BiDirectionalNode].inEdges(index) = node.id
                }
              }
            }
          }
          Future.join(futures)
        }
      }

      for {
        inEdgesSizes <- findInEdgesSizes(nodesOutEdges, nodeIdSet, numNodes)
        _ <- instantiateInEdges(inEdgesSizes)
        _ <- populateInEdges(inEdgesSizes)
      } yield ()
    }

    /**
     * Calculates sizes of incoming edges arrays.
     *
     * @return Future of an array of atomic integers holding number of incoming edges to node `i`
     *         on position `i`
     */
    private def findInEdgesSizes(nodesOutEdges: Seq[Seq[Node]],
                                 nodeIdSet: Array[Byte], maxNodeId: Int): Future[Array[AtomicInteger]] = {
      statsReceiver.time("graph_load_find_in_edge_sizes") {
        val atomicIntArray = Array.tabulate[AtomicInteger](maxNodeId + 1) {
          i => if (nodeIdSet(i) == 1) new AtomicInteger() else null
        }

        val futures = nodesOutEdges map {
          nodes => futurePool {
            nodes foreach {
              node => node.outboundNodes foreach { outEdge => atomicIntArray(outEdge).incrementAndGet()}
            }
          }
        }

        Future.join(futures).map(_ => atomicIntArray)
      }
    }
  }
}


/**
 * This class is an implementation of the directed graph trait that is backed by an array
 * The private constructor takes as its input a list of (@see Node) nodes, then stores
 * nodes in an array. It also builds all edges which are also stored in array.
 *
 * @param nodes the list of nodes with edges instantiated
 * @param maxId the max node id in the graph
 * @param nodeCount the number of nodes in the graph
 * @param edgeCount the number of edges in the graph
 * @param storedGraphDir the graph direction(s) stored
 */
class ArrayBasedDirectedGraph private (nodes: Array[Node], maxId: Int,
                              val nodeWithOutEdgesMaxId: Int,
                              val nodeWithOutEdgesCount: Int, val nodeCount: Int, val edgeCount: Long,
                              val storedGraphDir: StoredGraphDir) extends DirectedGraph {

  override lazy val maxNodeId = maxId

  def iterator = nodes.iterator.filter (_ != null)

  def getNodeById(id: Int) = {
    if ( (id < 0) || (id >= nodes.size)) {
      None
    } else {
      val node = nodes(id)
      if (node == null) {
        None
      } else {
        Some(node)
      }
    }
  }
}
