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
import com.twitter.cassovary.util.{BoundedFuturePool, Sharded2dArray}
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.{Await, Future, FuturePool}

/**
 * provides methods for constructing a shared array based graph
 */
object SharedArrayBasedDirectedGraph {
  private lazy val log = Logger.get()
  private val statsReceiver = DefaultStatsReceiver
  val emptyArray = Array.empty[Int]

  /**
   * Construct a shared array-based graph from a sequence of NodeIdEdgesMaxId iterables.
   * Eg each NodeIdEdgesMaxId could correspond to one graph dump file.
   *
   * This function builds the graph using similar steps as in ArrayBasedDirectedGraph.
   * The main difference here is that instead of each node has a separate edge array,
   * here one shared array is used, thus each node can find its edges through
   * an offset into this shared array. The avoid huge arrays, this edge array
   * is also sharded based on node's id.
   *
   * @param iterableSeq the sequence of nodes each with its own edges
   * @param parallelismLimit number of threads construction uses
   * @param storedGraphDir the direction of the graph to be built
   * @param numOfShards specifies the number of shards to use in creating shared array
   */
  def apply(iterableSeq: Seq[Iterable[NodeIdEdgesMaxId]], parallelismLimit: Int,
      storedGraphDir: StoredGraphDir, numOfShards: Int) = {
    val constructor = new SharedArrayBasedDirectedGraphConstructor(iterableSeq, parallelismLimit, storedGraphDir,
      numOfShards)
    constructor.construct()
  }

  private class SharedArrayBasedDirectedGraphConstructor(
    iterableSeq: Seq[Iterable[NodeIdEdgesMaxId]],
    parallelismLimit: Int,
    storedGraphDir: StoredGraphDir,
    numOfShards: Int
   ) {
    assert(numOfShards > 0)

    val nodeReadingLoggingFrequency = 10000

    private val futurePool: FuturePool = new BoundedFuturePool(FuturePool.unboundedPool, parallelismLimit)

    /**
     * Read everything to determine the total num of outgoing edges and max id for each shard.
     * An edge fall in a shard when the source node id is hashed to the index of the shard.
     *
     * @return shared graph meta-information object with filled all information but node count
     */
    private def readMetaInfoPerShard(sharedEdgeArraySizeCount: Array[AtomicInteger]):
    Future[Seq[SharedGraphMetaInfo]] = {
      log.info("read out num of edges and max id from files in parallel")
      statsReceiver.timeFuture("graph_load_read_out_edge_sizes_dump_files") {
        val futures = iterableSeq.map {
          edgesIterable => futurePool {
            var id, newMaxId, varNodeWithOutEdgesMaxId, numOfEdges, edgesLength, nodeCount = 0
            val iteratorForEdgeSizes = edgesIterable.iterator
            iteratorForEdgeSizes foreach { item =>
              id = item.id
              newMaxId = newMaxId max item.maxId
              varNodeWithOutEdgesMaxId = varNodeWithOutEdgesMaxId max id
              edgesLength = item.edges.length
              sharedEdgeArraySizeCount(id % numOfShards).addAndGet(edgesLength)
              numOfEdges += edgesLength
              nodeCount += 1
            }
            SharedGraphMetaInfo(newMaxId, varNodeWithOutEdgesMaxId, nodeCount, -1, numOfEdges)
          }
        }
        Future.collect(futures)
      }
    }

    /**
     * Aggregate meta-information from parts of graph
     */
    private def aggregateMetaInfoFromParts(partsMetaInfo: Seq[SharedGraphMetaInfo]):
      SharedGraphMetaInfo = {

      def aggregate(meta1: SharedGraphMetaInfo, meta2: SharedGraphMetaInfo) = {
        SharedGraphMetaInfo(meta1.maxId max meta2.maxId,
          meta1.nodeWithOutEdgesMaxId max meta2.nodeWithOutEdgesMaxId,
          meta1.nodeWithOutEdgesCount + meta2.nodeWithOutEdgesCount, -1,
          meta1.edgeCount + meta2.edgeCount)
      }

      partsMetaInfo.reduce(aggregate)
    }

    /**
     * Instantiates shared array. Resets `sharedEdgeArraySizeCount`.
     */
    private def instantiateSharedArray(sharedEdgeArraySizeCount: Array[AtomicInteger]):
    Array[Array[Int]] = {
      // instantiate shared array (2-dimensional)
      val sharedEdgeArray = new Array[Array[Int]](numOfShards)
      for (i <- 0 until numOfShards) {
        sharedEdgeArray(i) = new Array[Int](sharedEdgeArraySizeCount(i).get)
        // reset size counter
        sharedEdgeArraySizeCount(i).set(0)
      }
      sharedEdgeArray
    }

    private def fillEdgesMarkNodes(iterableSeq: Seq[Iterable[NodeIdEdgesMaxId]],
                           sharedEdgeArray: Array[Array[Int]], nodeIdSet: Array[Byte],
                           offsetTable: Array[Int], lengthTable: Array[Int],
                           nextFreeCellInSharedTable: Array[AtomicInteger]): Future[Unit] = {
      log.info("loading nodes and out edges from file in parallel " +
        "and mark the ids of all stored nodes in nodeIdSet")
      val nodesReadCounter = new AtomicInteger()
      statsReceiver.timeFuture("graph_dump_load_partial_nodes_and_out_edges_parallel") {
        val futures = iterableSeq.map {
          edgesIterable =>
            statsReceiver.timeFuture("graph_load_read_out_edge_from_dump_files") {
              futurePool {
                var id, edgesLength, shardIdx, offset = 0
                edgesIterable.foreach { item =>
                  id = item.id
                  nodeIdSet(id) = 1
                  edgesLength = item.edges.length
                  shardIdx = id % numOfShards
                  offset = nextFreeCellInSharedTable(shardIdx).getAndAdd(edgesLength)
                  Array.copy(item.edges, 0, sharedEdgeArray(shardIdx), offset, edgesLength)
                  offsetTable(id) = offset
                  lengthTable(id) = edgesLength
                  item.edges foreach { edge => nodeIdSet(edge) = 1}
                  val c = nodesReadCounter.addAndGet(1)
                  if (c % nodeReadingLoggingFrequency == 0) {
                    log.info("loading..., finished %s nodes.", c)
                  }
                }
              }
            }
        }
        Future.join(futures)
      }
    }

    private def countNodes(nodeIdSet: Array[Byte], maxNodeId: Int): Int = {
      var n: Int = 1
      for (i <- 0 to maxNodeId) n += nodeIdSet(i)
      n
    }

    def construct(): SharedArrayBasedDirectedGraph = {
      // initialize size counters for each shard in shared array
      val sharedEdgeArraySizeCount = new Array[AtomicInteger](numOfShards)
      for (i <- 0 until numOfShards) sharedEdgeArraySizeCount(i) = new AtomicInteger()

      val future = for {
        partsMetaInfo <- readMetaInfoPerShard(sharedEdgeArraySizeCount)
        metaInfo = aggregateMetaInfoFromParts(partsMetaInfo)
        maxId = metaInfo.maxId
        sharedEdgeArray = instantiateSharedArray(sharedEdgeArraySizeCount)
        nodeIdSet = new Array[Byte](maxId + 1)
        offsetTable = new Array[Int](maxId + 1)
        lengthTable = new Array[Int](maxId + 1)
        _ <- fillEdgesMarkNodes(iterableSeq, sharedEdgeArray, nodeIdSet, offsetTable, lengthTable,
          sharedEdgeArraySizeCount)
        edges = Sharded2dArray.fromArrays(sharedEdgeArray, nodeIdSet, offsetTable, lengthTable)
        _ = metaInfo.nodeCount = countNodes(nodeIdSet, maxId)
        reverseDirEdgeArray <-
          if (storedGraphDir == BothInOut)
            createReverseDirEdgeArray(iterableSeq, sharedEdgeArray, sharedEdgeArraySizeCount,
              nodeIdSet, offsetTable, lengthTable, maxId).map(x => Some(x))
          else
            Future.value(None)
      } yield new SharedArrayBasedDirectedGraph(nodeIdSet, edges,
          reverseDirEdgeArray, metaInfo, storedGraphDir)

      Await.result(future)
    }

    /**
     * Creates an array for reverse direction edges.
     *
     * Needed only if storing both directions.
     */
    private def createReverseDirEdgeArray(iterableSeq: Seq[Iterable[NodeIdEdgesMaxId]],
                                  sharedEdgeArray: Array[Array[Int]], shardsLengths: Array[AtomicInteger],
                                  nodeIdSet: Array[Byte],
                                  offsetTable: Array[Int], lengthTable: Array[Int], maxNodeId: Int):
    Future[Sharded2dArray[Int]] = {

      def findInEdgesAndShardsSizes(sharedEdgeArray: Array[Array[Int]], shardsLengths: Array[AtomicInteger],
                                    nodeIdSet: Array[Byte]): Future[(Array[AtomicInteger], Array[AtomicInteger])] = {
        log.info("calculating in edges shards sizes")
        statsReceiver.timeFuture("graph_load_find_in_edge_shards_sizes") {
          val inEdgesSizes = new Array[AtomicInteger](maxNodeId + 1)
          val inEdgesShardsSizes = Array.fill(numOfShards)(new AtomicInteger())
          for (id <- 0 to maxNodeId) {
            if (nodeIdSet(id) == 1) inEdgesSizes(id) = new AtomicInteger()
          }
          val futures = (0 until numOfShards).map {
            shard => futurePool {
              (0 until shardsLengths(shard).get()).foreach {
                idx =>
                  inEdgesSizes(sharedEdgeArray(shard)(idx)).incrementAndGet()
                  inEdgesShardsSizes(sharedEdgeArray(shard)(idx) % numOfShards).incrementAndGet()
              }
            }
          }
          Future.join(futures).map(_ => (inEdgesSizes, inEdgesShardsSizes))
        }
      }

      /**
       * Fills lengths and offsets using inEdgesSizes and nextFreeCellPerShard. Resets
       * `inEdgesSizes`.
       *
       * Assumes that nextFreeCellPerShard is 0 everywhere.
       */
      def fillInEdgesLengthsAndOffsets(lengthTable: Array[Int], offsetTable: Array[Int],
                                       inEdgesSizes: Array[AtomicInteger],
                                       nextFreeCellPerShard: Array[AtomicInteger]): Future[Unit] = {
        log.info("filling lengths and offsets")
        statsReceiver.timeFuture("graph_load_fill_in_edge_lengths_and_offsets") {
          val futures = (0 until numOfShards).map {
            shard => futurePool {
              // we assume modulo hashing below
              (shard until (maxNodeId + 1) by numOfShards).foreach {
                id =>
                  if (nodeIdSet(id) == 1) {
                    lengthTable(id) = if (inEdgesSizes(id) != null) inEdgesSizes(id).get else 0
                    if (lengthTable(id) > 0)
                      offsetTable(id) = nextFreeCellPerShard(shard).getAndAdd(lengthTable(id))
                    inEdgesSizes(id).set(0)
                  }
              }
            }
          }
          Future.join(futures)
        }
      }

      /**
       * Fills in edges assuming that lengthTable and offsetTable are filled and
       * nextFreeCell is 0 everywhere.
       */
      def fillInEdges(sharedInEdgesArray: Array[Array[Int]], offsetTable: Array[Int],
                      lengthTable: Array[Int], nextFreeCellPerNode: Array[AtomicInteger]):
      Future[Unit] = {
        log.info("filling in edges")
        statsReceiver.timeFuture("graph_load_fill_in_edges") {
          val futures: Seq[Future[Unit]] = iterableSeq.map {
            nodesMaxIdsSeq => futurePool {
              nodesMaxIdsSeq.foreach {
                item =>
                  item.edges.foreach {
                    target =>
                      sharedInEdgesArray(target % numOfShards)(offsetTable(target) +
                        nextFreeCellPerNode(target).getAndIncrement) = item.id
                  }
              }
            }
          }
          Future.join(futures)
        }
      }

      for {
        (inEdgesSizes, inEdgesShardsSizes) <- findInEdgesAndShardsSizes(sharedEdgeArray, shardsLengths, nodeIdSet)
        sharedInEdges = instantiateSharedArray(inEdgesShardsSizes)
        offsetInEdgesTable = new Array[Int](maxNodeId + 1)
        lengthInEdgesTable = new Array[Int](maxNodeId + 1)
        _ <- fillInEdgesLengthsAndOffsets(lengthInEdgesTable, offsetInEdgesTable, inEdgesSizes, inEdgesShardsSizes)
        _ <- fillInEdges(sharedInEdges, offsetInEdgesTable, lengthInEdgesTable, inEdgesSizes)
      } yield Sharded2dArray.fromArrays(sharedInEdges, nodeIdSet, offsetInEdgesTable, lengthInEdgesTable)
    }
  }

  @VisibleForTesting
  def apply(iterable: Iterable[NodeIdEdgesMaxId], storedGraphDir: StoredGraphDir):
    SharedArrayBasedDirectedGraph = apply(Seq(iterable), 1, storedGraphDir, 1)
}



/**
 * Contains meta-information about a particular graph instance.
 *
 * @param maxId the max node id in the graph
 * @param nodeWithOutEdgesMaxId the max id for nodes with non empty out edges
 * @param nodeWithOutEdgesCount the number of nodes with non empty out edges
 * @param nodeCount the number of nodes in the graph
 * @param edgeCount the number of edges in the graph
 */
case class SharedGraphMetaInfo(maxId: Int, nodeWithOutEdgesMaxId: Int,
                               nodeWithOutEdgesCount: Int, var nodeCount: Int, edgeCount: Long)

/**
 * This class is an implementation of the directed graph trait that is backed
 * by a sharded 2-dimensional edges array. Each node's edges are stored
 * consecutively in one shard of the edge array. Number of shard is usually much
 * smaller than number of nodes.
 *
 * @param nodeIdSet the nodes with either outgoing or incoming edges
 * @param edges the 2-dimensional sharded edge array
 * @param reverseDirEdges the reverse edge array, one per node
 * @param metaInformation graph meta-information
 */
class SharedArrayBasedDirectedGraph private (
  nodeIdSet: Array[Byte],
  edges: Sharded2dArray[Int],
  reverseDirEdges: Option[Sharded2dArray[Int]],
  metaInformation: SharedGraphMetaInfo,
  val storedGraphDir: StoredGraphDir
) extends DirectedGraph[Node] {

  override def nodeCount: Int = metaInformation.nodeCount

  override def edgeCount: Long = metaInformation.edgeCount

  override lazy val maxNodeId = metaInformation.maxId

  def iterator = (0 to maxId).flatMap(getNodeById).iterator

  def getNodeById(id: Int) = {
    if ((id < 0) || (id >= nodeIdSet.size) || (nodeIdSet(id) == 0)) {
      None
    } else {
      Some(SharedArrayBasedDirectedNode(id,
        edges, storedGraphDir, reverseDirEdges))
    }
  }
}
