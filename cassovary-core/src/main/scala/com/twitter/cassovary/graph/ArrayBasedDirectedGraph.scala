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
import com.google.common.util.concurrent.MoreExecutors
import com.twitter.cassovary.graph.node._
import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.util.ExecutorUtils
import com.twitter.logging.Logger
import com.twitter.ostrich.stats.Stats
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService, Future}
import scala.collection.mutable

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
private case class NodesMaxIds(nodesInOneThread: Seq[Node],
    maxIdInPart: Int, nodeWithOutEdgesMaxIdInPart: Int)

/**
 * provides methods for constructing an array based graph
 */
object ArrayBasedDirectedGraph {
  private lazy val log = Logger.get

  /**
   * Construct an array-based graph from an sequence of iterators over NodeIdEdgesMaxId
   * This function builds the array-based graph from a seq of nodes with out edges
   * using the following steps:
   * 0. read from file and construct a sequence of Nodes
   * 1. create an array of size maxNodeid
   * 2. mark all positions in the array where there is a node
   * 3. instantiate nodes that only have in-edges (thus has not been created in the input)
   * next steps apply only if in-edge is needed
   * 4. calculate in-edge array sizes
   * 5. (if in-edge dir only) remove out edges
   * 6. instantiate in-edge arrays
   * 7. iterate over the sequence of nodes again, instantiate in-edges
   */
  def apply(iteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ], executorService: ExecutorService,
      storedGraphDir: StoredGraphDir) = {

    val nodesOutEdges = new mutable.ArrayBuffer[Seq[Node]]
    var maxNodeId = 0
    var nodeWithOutEdgesMaxId = 0
    var numEdges = 0L
    var numNodes = 0

    log.debug("loading nodes and out edges from file in parallel")
    val futures = Stats.time("graph_dump_load_partial_nodes_and_out_edges_parallel") {
      def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) =
          Stats.time("graph_load_read_out_edge_from_dump_files") {
        val nodes = new mutable.ArrayBuffer[Node]
        var newMaxId = 0
        var varNodeWithOutEdgesMaxId = 0
        var id = 0
        var edgesLength = 0
        var edges: Array[Int] = Array.empty[Int]

        val iterator = iteratorFunc()
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

      ExecutorUtils.parallelWork[ () => Iterator[NodeIdEdgesMaxId], NodesMaxIds](executorService,
          iteratorSeq, readOutEdges)
    }

    futures.toArray map { future =>
      val f = future.asInstanceOf[Future[NodesMaxIds]]
      val NodesMaxIds(nodesInOneThread, maxIdInPart, nodeWithOutEdgesMaxIdInPart) = f.get
      nodesOutEdges += nodesInOneThread
      maxNodeId = maxNodeId max maxIdInPart
      nodeWithOutEdgesMaxId = nodeWithOutEdgesMaxId max nodeWithOutEdgesMaxIdInPart
    }

    val nodeIdSet = new Array[Byte](maxNodeId + 1)
    val table = new Array[Node](maxNodeId + 1)

    log.debug("mark the ids of all stored nodes in nodeIdSet")
    Stats.time("graph_load_mark_ids_of_stored_nodes") {
      def markAllNodes = {
        (nodes: Seq[Node]) => {
          nodes foreach { node =>
            val nodeId = node.id
            if (table(nodeId) != null)
              log.warning("Duplicate node detected. (" + nodeId + ")")
            table(nodeId) = node
            nodeIdSet(nodeId) = 1
            storedGraphDir match {
              case StoredGraphDir.OnlyIn =>
                node.inboundNodes foreach { inEdge => nodeIdSet(inEdge) = 1 }
              case _ =>
                node.outboundNodes foreach { outEdge => nodeIdSet(outEdge) = 1 }
            }
          }
        }
      }
      ExecutorUtils.parallelForEach[Seq[Node], Unit](executorService, nodesOutEdges, markAllNodes)
    }

    // creating nodes that have only in edges but no out edges
    // also calculates the total number of edges
    val nodesWithNoOutEdges = new mutable.ArrayBuffer[Node]
    var nodeWithOutEdgesCount = 0
    log.debug("creating nodes that have only in-coming edges")
    Stats.time("graph_load_creating_nodes_without_out_edges") {
      for ( id <- 0 to maxNodeId ) {
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
                numEdges += table(id).inboundNodes.size
              case _ =>
                numEdges += table(id).outboundNodes.size
            }
          }
        }
      }
    }

    // the work below is needed for BothInOut directions only
    if (storedGraphDir == StoredGraphDir.BothInOut) {
      val allNodes = new mutable.ArrayBuffer[Seq[Node]]
      allNodes ++= nodesOutEdges
      allNodes += nodesWithNoOutEdges

      log.debug("calculating in edges sizes")
      val inEdgesSizes = findInEdgesSizes()

      def populateInEdges() {
        def readInEdges = Stats.time("graph_load_read_in_edge_from_dump_files") {
          (nodes: Seq[Node]) => {
            nodes foreach { node =>
              node.outboundNodes foreach { outEdge =>
                val index = inEdgesSizes(outEdge).getAndIncrement()
                table(outEdge).asInstanceOf[BiDirectionalNode].inEdges(index) = node.id
              }
            }
          }
        }
        ExecutorUtils.parallelForEach[Seq[Node], Unit](executorService, nodesOutEdges, readInEdges)
      }

      def instantiateInEdges() {
        Stats.time("graph_load_instantiate_in_edge_arrays") {
          def instantiateInEdgesTask = {
            (nodes: Seq[Node]) => {
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
          }

          ExecutorUtils.parallelForEach[Seq[Node], Unit](executorService, allNodes,
              instantiateInEdgesTask)
        }
      }

      log.debug("instantiating in edge arrays")
      instantiateInEdges()

      log.debug("populate in edges")
      populateInEdges()
    }

    def findInEdgesSizes() = Stats.time("graph_load_find_in_edge_sizes") {
      val atomicIntArray = new Array[AtomicInteger](maxNodeId + 1)
      var id = 0
      while (id <= maxNodeId) {
        if (nodeIdSet(id) == 1) {
          atomicIntArray(id) = new AtomicInteger()
        }
        id += 1
      }
      def findInEdgeSizesTask = {
        (nodes: Seq[Node]) => {
          nodes foreach { node =>
            node.outboundNodes foreach { outEdge => atomicIntArray(outEdge).incrementAndGet() }
          }
        }
      }
      ExecutorUtils.parallelForEach[Seq[Node], Unit](executorService,
          nodesOutEdges, findInEdgeSizesTask)
      atomicIntArray
    }

    new ArrayBasedDirectedGraph(table.asInstanceOf[Array[Node]], maxNodeId,
      nodeWithOutEdgesMaxId, nodeWithOutEdgesCount, numNodes, numEdges, storedGraphDir)
  }
  @VisibleForTesting
  def apply( iteratorFunc: () => Iterator[NodeIdEdgesMaxId],
        storedGraphDir: StoredGraphDir): ArrayBasedDirectedGraph =
    apply(Seq(iteratorFunc), MoreExecutors.sameThreadExecutor(), storedGraphDir)
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

  def iterator = nodes.iterator.filter { _ != null }

  def getNodeById(id: Int) = {
    if (id >= nodes.size) {
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
