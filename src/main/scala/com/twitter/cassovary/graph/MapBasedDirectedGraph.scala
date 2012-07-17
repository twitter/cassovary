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

import com.google.common.annotations.VisibleForTesting
import com.google.common.util.concurrent.MoreExecutors
import com.twitter.cassovary.graph.node._
import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.util.ExecutorUtils
import com.twitter.ostrich.stats.Stats
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService, Future}
import net.lag.logging.Logger
import scala.collection.immutable
import scala.collection.mutable

/**
* MapBasedDirectedGraph is very similar to ArrayBasedDirectedGraph, but potentially
* more suited for graphs with extremely large node id's that are non-sequential and
* do not span the entire range of 0 - |V|. The Map circumvents the need
* to create an extremely large Array in such a case. Different from SynchronizedDyanmicGraph
* in that it is immutable. Due to how hash maps are implemented however, it is not
* as memory efficient as ArrayBasedDirectedGraph.
*/
object MapBasedDirectedGraph {
  private lazy val log = Logger.get
  
  def apply(iteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ], executorService: ExecutorService,
      storedGraphDir: StoredGraphDir) = {

    val nodesOutEdges = new mutable.ArrayBuffer[Seq[Node]]
    var numEdges = 0L
    var numNodes = 0

    log.debug("loading nodes and out edges from file in parallel")
    val futures = Stats.time("graph_dump_load_partial_nodes_and_out_edges_parallel") {
      def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) =
          Stats.time("graph_load_read_out_edge_from_dump_files") {
        val nodes = new mutable.ArrayBuffer[Node]
        var id = 0
        var edgesLength = 0
        var edges: Array[Int] = Array.empty[Int]

        val iterator = iteratorFunc()
        iterator foreach { item =>
          id = item.id
          val edges = item.edges
          edgesLength = edges.length
          val newNode = ArrayBasedDirectedNode(id, edges, storedGraphDir)
          nodes += newNode
        }
        NodesMaxIds(nodes, 0, 0)
      }

      ExecutorUtils.parallelWork[ () => Iterator[NodeIdEdgesMaxId], NodesMaxIds](executorService,
          iteratorSeq, readOutEdges)
    }

    futures.toArray map { future =>
      val f = future.asInstanceOf[Future[NodesMaxIds]]
      val NodesMaxIds(nodesInOneThread, _, _) = f.get
      nodesOutEdges += nodesInOneThread
    }

    val nodeIdSet = new mutable.HashSet[Int]
    val table = new mutable.HashMap[Int, Node]

    log.debug("mark the ids of all stored nodes in nodeIdSet")
    Stats.time("graph_load_mark_ids_of_stored_nodes") {
      def markAllNodes = {
        (nodes: Seq[Node]) => {
          nodes foreach { node =>
            val nodeId = node.id
            table(nodeId) = node
            nodeIdSet add nodeId
            storedGraphDir match {
              case StoredGraphDir.OnlyIn =>
                node.inboundNodes foreach { inEdge => nodeIdSet add inEdge }
              case _ =>
                node.outboundNodes foreach { outEdge => nodeIdSet add outEdge }
            }
          }
        }
      }
      ExecutorUtils.parallelWork[Seq[Node], Unit](executorService, nodesOutEdges, markAllNodes)
    }

    // creating nodes that have only in edges but no out edges
    // also calculates the total number of edges
    val nodesWithNoOutEdges = new mutable.ArrayBuffer[Node]
    var nodeWithOutEdgesCount = 0
    log.debug("creating nodes that have only in-coming edges")
    Stats.time("graph_load_creating_nodes_without_out_edges") {
      for (id <- nodeIdSet) {
        numNodes += 1
        if (!(table contains id)) {
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
        ExecutorUtils.parallelWork[Seq[Node], Unit](executorService, nodesOutEdges, readInEdges)
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

          ExecutorUtils.parallelWork[Seq[Node], Unit](executorService, allNodes,
              instantiateInEdgesTask)
        }
      }

      log.debug("instantiating in edge arrays")
      instantiateInEdges()

      log.debug("populate in edges")
      populateInEdges()
    }

    def findInEdgesSizes() = Stats.time("graph_load_find_in_edge_sizes") {
      val atomicIntArray = new mutable.HashMap[Int, AtomicInteger]
      for (id <- nodeIdSet) atomicIntArray(id) = new AtomicInteger()
      def findInEdgeSizesTask = {
        (nodes: Seq[Node]) => {
          nodes foreach { node =>
            node.outboundNodes foreach { outEdge => atomicIntArray(outEdge).incrementAndGet() }
          }
        }
      }
      ExecutorUtils.parallelWork[Seq[Node], Unit](executorService,
          nodesOutEdges, findInEdgeSizesTask)
      atomicIntArray
    }

    new MapBasedDirectedGraph(table.toMap, numNodes, numEdges, storedGraphDir)
  }
  @VisibleForTesting
  def apply( iteratorFunc: () => Iterator[NodeIdEdgesMaxId],
        storedGraphDir: StoredGraphDir): MapBasedDirectedGraph =
    apply(Seq(iteratorFunc), MoreExecutors.sameThreadExecutor(), storedGraphDir)
}

/**
* This class is an implementation of the directed graph trait that is backed by a map
* The private constructor takes as its input a map of the integer node id to the corresponding
* Node object, and stores it in an adjacency list style format.
*
* @param nodes the map where key = integer id and value = corresponding Node object 
* @param nodeCount the number of nodes in the graph
* @param edgeCount the number of edges in the graph
* @param storedGraphDir the graph direction(s) stored
*/
class MapBasedDirectedGraph private (val nodes: immutable.Map[Int, Node], 
    val nodeCount: Int, val edgeCount: Long, val storedGraphDir: StoredGraphDir) extends DirectedGraph {
      
  override lazy val maxNodeId = nodes.keys.max
  
  override def iterator = nodes.valuesIterator
  
  override def getNodeById(id: Int) = nodes.get(id) 
}
