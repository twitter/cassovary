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
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ExecutorService, Future}

/**
 * provides methods for constructing a shared array based graph
 */
object SharedArrayBasedDirectedGraph {
  private lazy val log = Logger.get
  val emptyArray = Array.empty[Int]

  /**
   * Construct a shared array-based graph from a sequence of iterators over NodeIdEdgesMaxId.
   * Eg each NodeIdEdgesMaxId could correspond to one graph dump file.
   *
   * This function builds the graph using similar steps as in ArrayBasedDirectedGraph.
   * The main difference here is that instead of each node has a separate edge array,
   * here one shared array is used, thus each node can find its edges through
   * an offset into this shared array. The avoid huge arrays, this edge array
   * is also sharded based on node's id.
   *
   * @param iteratorSeq the sequence of nodes each with its own edges
   * @param executorService the executor for parallel execution
   * @param storedGraphDir the direction of the graph to be built
   * @param numOfShards specifies the number of shards to use in creating shared array
   */
  def apply(iteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ], executorService: ExecutorService,
      storedGraphDir: StoredGraphDir, numOfShards: Int) = {

    assert(numOfShards > 0)

    var maxNodeId = 0
    var nodeWithOutEdgesMaxId = 0
    var nodeWithOutEdgesCount = 0
    var numEdges = 0L
    var numNodes = 0

    // initialize size counters for each shard in shared array
    val sharedEdgeArraySizeCount = new Array[AtomicInteger](numOfShards)
    for (i <- 0 until numOfShards) sharedEdgeArraySizeCount(i) = new AtomicInteger()

    /* Read everything to determine the total num of outgoing edges and max id for each shard.
     * An edge fall in a shard when the source node id is hashed to the index of the shard.
     */
    log.info("read out num of edges and max id from files in parallel")
    val futures = Stats.time("graph_dump_read_out_num_of_edge_and_max_id_parallel") {
      def readNumOfEdgesAndMaxId(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) =
          Stats.time("graph_load_read_out_edge_sizes_dump_files") {
        var id, newMaxId, varNodeWithOutEdgesMaxId, numOfEdges, edgesLength, nodeCount = 0
        val iteratorForEdgeSizes = iteratorFunc()
        iteratorForEdgeSizes foreach { item =>
          id = item.id
          newMaxId = newMaxId max item.maxId
          varNodeWithOutEdgesMaxId = varNodeWithOutEdgesMaxId max id
          edgesLength = item.edges.length
          sharedEdgeArraySizeCount(id % numOfShards).addAndGet(edgesLength)
          numOfEdges += edgesLength
          nodeCount += 1
        }
        (newMaxId, varNodeWithOutEdgesMaxId, numOfEdges, nodeCount)
      }

      ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], (Int, Int, Int, Int)](
          executorService, iteratorSeq, readNumOfEdgesAndMaxId)
    }
    futures.toArray map { future =>
      val f = future.asInstanceOf[Future[(Int, Int, Int, Int)]]
      val (maxIdInPart, nodeWithOutEdgesMaxIdInPart, numOfEdgesInPart, nodeCountInPart) = f.get
      maxNodeId = maxNodeId max maxIdInPart
      nodeWithOutEdgesMaxId = nodeWithOutEdgesMaxId max nodeWithOutEdgesMaxIdInPart
      numEdges += numOfEdgesInPart
      nodeWithOutEdgesCount += nodeCountInPart
    }

    // instantiate shared array (2-dimensional)
    val sharedEdgeArray = new Array[Array[Int]](numOfShards)
    for (i <- 0 until numOfShards) {
      sharedEdgeArray(i) = new Array[Int](sharedEdgeArraySizeCount(i).get)
      // reset size counter
      sharedEdgeArraySizeCount(i).set(0)
    }

    val nodeIdSet = new Array[Byte](maxNodeId + 1)
    val offsetTable = new Array[Int](maxNodeId + 1)
    val lengthTable = new Array[Int](maxNodeId + 1)

    // read everything second time
    log.info("loading nodes and out edges from file in parallel " +
      "and mark the ids of all stored nodes in nodeIdSet")
    Stats.time("graph_dump_load_partial_nodes_and_out_edges_parallel") {
      var loadingCounter = new AtomicLong()
      val outputMode = 10000
      def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) =
          Stats.time("graph_load_read_out_edge_from_dump_files") {
        var id, edgesLength, shardIdx, offset = 0

        val iterator = iteratorFunc()

        iterator foreach { item =>
          id = item.id
          nodeIdSet(id) = 1
          edgesLength = item.edges.length
          shardIdx = id % sharedEdgeArray.length
          offset = sharedEdgeArraySizeCount(shardIdx).getAndAdd(edgesLength)
          Array.copy(item.edges, 0, sharedEdgeArray(shardIdx), offset, edgesLength)
          offsetTable(id) = offset
          lengthTable(id) = edgesLength
          item.edges foreach { edge => nodeIdSet(edge) = 1 }
          val c = loadingCounter.addAndGet(1)
          if (c % outputMode == 0) {
            log.info("loading..., finished %s nodes.", loadingCounter)
          }
        }
      }

      ExecutorUtils.parallelForEach[() => Iterator[NodeIdEdgesMaxId], Unit](
          executorService, iteratorSeq, readOutEdges)
    }

    log.info("Count total number of nodes")
    Stats.time("graph_load_count_total_num_of_nodes") {
      for ( id <- 0 to maxNodeId )
        if (nodeIdSet(id) == 1)
          numNodes += 1
    }

    // the work below is needed for BothInOut directions only
    val reverseDirEdgeArray = if (storedGraphDir == StoredGraphDir.BothInOut) {
      log.info("calculating in edges sizes")
      val inEdgesSizes = Stats.time("graph_load_find_in_edge_sizes") {
        val atomicIntArray = new Array[AtomicInteger](maxNodeId + 1)
        for (id <- 0 to maxNodeId) {
          if (nodeIdSet(id) == 1) atomicIntArray(id) = new AtomicInteger()
        }
        def findInEdgeSizesTask = {
          (shard: Int) => {
            var id, offset, length, edgeIndex = 0
            for(i <- 0 to (maxNodeId / numOfShards)) {
              id = i * numOfShards + shard
              if (id <= maxNodeId && nodeIdSet(id) == 1) {
                offset = offsetTable(id)
                length = lengthTable(id)
                for (edgeOffset <- offset until (offset + length)) {
                  edgeIndex = sharedEdgeArray(id % numOfShards)(edgeOffset)
                  atomicIntArray(edgeIndex).incrementAndGet()
                }
              }
            }
          }
        }
        ExecutorUtils.parallelForEach[Int, Unit](
            executorService, (0 until numOfShards), findInEdgeSizesTask)
        atomicIntArray
      }

      log.info("instantiating in edge arrays")
      val reverseEdges = new Array[Array[Int]](maxNodeId + 1)

      Stats.time("graph_load_instantiate_in_edge_arrays") {
        def instantiateInEdgesTask = {
          (shard: Int) => {
            var id, edgeSize = 0
            for(i <- 0 to (maxNodeId / numOfShards)) {
              id = i * numOfShards + shard
              if ((id <= maxNodeId) && (nodeIdSet(id) == 1)) {
                edgeSize = inEdgesSizes(id).intValue()
                if (edgeSize > 0) reverseEdges(id) = new Array[Int](edgeSize)
                // reset inEdgesSizes, and use it as index pointer of
                // the current insertion place when adding in edges
                inEdgesSizes(id).set(0)
              }
            }
          }
        }

        ExecutorUtils.parallelForEach[Int, Unit](
            executorService, (0 until numOfShards), instantiateInEdgesTask)
      }

      log.info("populate in edges")
      Stats.time("graph_load_read_in_edge_from_dump_files") {
        def readInEdges = {
          (shard: Int) => {
            var id, offset, length, edgeIndex, index = 0
            for(i <- 0 to (maxNodeId / numOfShards)) {
              id = i * numOfShards + shard
              if ((id <= maxNodeId) && (nodeIdSet(id) == 1)) {
                offset = offsetTable(id)
                length = lengthTable(id)
                for (edgeOffset <- offset until (offset + length)) {
                  edgeIndex = sharedEdgeArray(id % numOfShards)(edgeOffset)
                  index = inEdgesSizes(edgeIndex).getAndIncrement()
                  reverseEdges(edgeIndex)(index) = id
                }
              }
            }
          }
        }
        ExecutorUtils.parallelForEach[Int, Unit](executorService, (0 until numOfShards), readInEdges)
      }
      Some(reverseEdges)
    } else None

    new SharedArrayBasedDirectedGraph(nodeIdSet, offsetTable, lengthTable, sharedEdgeArray,
      reverseDirEdgeArray, maxNodeId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
      numNodes, numEdges, storedGraphDir)
  }

  @VisibleForTesting
  def apply( iteratorFunc: () => Iterator[NodeIdEdgesMaxId], storedGraphDir: StoredGraphDir):
    SharedArrayBasedDirectedGraph = apply(Seq(iteratorFunc), MoreExecutors.sameThreadExecutor(),
      storedGraphDir, 1)
}


/**
 * This class is an implementation of the directed graph trait that is backed
 * by a sharded 2-dimensional edges array. Each node's edges are stored
 * consecutively in one shard of the edge array. Number of shard is usually much
 * smaller than number of nodes.
 *
 * @param nodeIdSet the nodes with either outgoing or incoming edges
 * @param offsetTable the offset into shared edge array for outgoing edges
 * @param lengthTable the number of outgoing edges for each node
 * @param sharedEdgeArray the 2-dimensional sharded edge array
 * @param reverseDirEdgeArray the reverse edge array, one per node
 * @param maxId the max node id in the graph
 * @param nodeWithOutEdgesMaxId the max id for nodes with non empty out edges
 * @param nodeWithOutEdgesCount the number of nodes with non empty out edges
 * @param nodeCount the number of nodes in the graph
 * @param edgeCount the number of edges in the graph
 * @param storedGraphDir the graph direction(s) stored
 */
class SharedArrayBasedDirectedGraph private (nodeIdSet: Array[Byte], offsetTable: Array[Int],
    lengthTable: Array[Int], sharedEdgeArray: Array[Array[Int]],
    reverseDirEdgeArray: Option[Array[Array[Int]]], maxId: Int, val nodeWithOutEdgesMaxId: Int,
    val nodeWithOutEdgesCount: Int, val nodeCount: Int, val edgeCount: Long,
    val storedGraphDir: StoredGraphDir) extends DirectedGraph {

  override lazy val maxNodeId = maxId

  def iterator = (0 to maxId).flatMap(getNodeById(_)).iterator

  def getNodeById(id: Int) = {
    if ((id >= nodeIdSet.size) || (nodeIdSet(id) == 0)) {
      None
    } else {
      val reverseEdges = reverseDirEdgeArray match {
        case None => SharedArrayBasedDirectedGraph.emptyArray
        case Some(reverse) =>
          if (reverse(id) != null) reverse(id)
          else SharedArrayBasedDirectedGraph.emptyArray
      }
      Some(SharedArrayBasedDirectedNode(id, offsetTable(id), lengthTable(id),
        sharedEdgeArray, storedGraphDir, Some(reverseEdges)))
    }
  }
}
